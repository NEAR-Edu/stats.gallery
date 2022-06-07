use std::{collections::HashSet, str::FromStr, sync::Arc};

use chrono::Duration;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use lazy_static::lazy_static;
use near_jsonrpc_client::{errors::JsonRpcError, methods::query::RpcQueryError};
use near_primitives::types::AccountId;
use num_traits::FromPrimitive;
use sqlx::{
    types::{Decimal, Uuid},
    FromRow, PgPool,
};
use tokio::{
    join,
    sync::{mpsc, oneshot},
};
use tracing::{error, info, warn};

/// Minimum amount of time between account updates
const ACCOUNT_UPDATE_COOLDOWN_MINUTES: i64 = 60 * 6;
/// Doubles every failure
const ACCOUNT_UPDATE_FAILURE_PENALTY_COEFFICIENT_MINUTES: i64 = 60 * 12;

lazy_static! {
    static ref NULL_ACCOUNT: AccountId = AccountId::from_str(&"0".repeat(64)).unwrap();
}

use crate::{
    connections::Connections,
    indexer::{calculate_account_score, ScoreCalculationError},
    rpc::get_account_balance,
};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum UpdateAccountError {
    #[error("Error calculating score: {0}")]
    Score(#[from] ScoreCalculationError),
    #[error("Error retrieving balance: {0}")]
    Balance(#[from] JsonRpcError<RpcQueryError>),
    #[error("Error running account update query: {0}")]
    InsertQuery(#[from] sqlx::Error),
}

#[derive(FromRow)]
pub struct AccountRecordDb {
    pub id: String,
    pub balance: Option<Decimal>,
    pub score: Option<i32>,
    pub modified: Option<chrono::DateTime<chrono::Utc>>,
    pub consecutive_errors: Option<i16>,
}

pub struct AccountRecord {
    pub id: AccountId,
    pub balance: Option<u128>,
    pub score: Option<u32>,
    pub modified: Option<chrono::DateTime<chrono::Utc>>,
    pub consecutive_errors: u16,
}

impl AccountRecord {
    pub fn next_update_allowed_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.modified.map(|m| {
            m + (Duration::minutes(ACCOUNT_UPDATE_COOLDOWN_MINUTES))
                + (Duration::minutes(ACCOUNT_UPDATE_FAILURE_PENALTY_COEFFICIENT_MINUTES)
                    * ((1 << self.consecutive_errors) - 1))
        })
    }
}

impl From<AccountRecordDb> for AccountRecord {
    fn from(record: AccountRecordDb) -> Self {
        Self {
            id: record
                .id
                .parse()
                .unwrap_or_else(|_| NULL_ACCOUNT.to_owned()),
            balance: record.balance.and_then(|b| u128::try_from(b).ok()),
            score: record.score.map(|s| s as u32),
            modified: record.modified,
            consecutive_errors: record.consecutive_errors.unwrap_or(0) as u16,
        }
    }
}

#[tracing::instrument(skip(local_pool))]
pub async fn query_account(
    local_pool: &PgPool,
    account_id: &AccountId,
) -> Result<AccountRecord, sqlx::Error> {
    sqlx::query_as!(
        AccountRecordDb,
        r#"--sql
select id, balance, score, modified, consecutive_errors
    from account
    where id = $1
    "#,
        account_id.to_string()
    )
    .fetch_one(local_pool)
    .map(|a| a.map(AccountRecord::from))
    .await
}

#[derive(FromRow)]
pub struct AccountBadgeRecordDb {
    pub badge_id: Uuid,
}

#[tracing::instrument(skip(local_pool))]
pub async fn add_badge_for_account(
    local_pool: &PgPool,
    account_id: &AccountId,
    badge_id: &Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"--sql
insert into account_badge(account_id, badge_id)
    values ($1, $2)
    on conflict (account_id, badge_id) do nothing
"#,
        account_id.to_string(),
        &badge_id
    )
    .execute(local_pool)
    .map_ok(|_| ())
    .await
}

#[tracing::instrument(skip(local_pool))]
pub async fn get_badges_for_account(
    local_pool: &PgPool,
    account_id: &AccountId,
) -> Result<HashSet<Uuid>, sqlx::Error> {
    sqlx::query_as!(
        AccountBadgeRecordDb,
        r#"--sql
select badge_id
    from account_badge
    where account_id = $1
"#,
        account_id.to_string()
    )
    .fetch(local_pool)
    .map(|b| b.map(|a| a.badge_id))
    .try_collect::<HashSet<Uuid>>()
    .await
}

#[tracing::instrument(skip(connections))]
pub async fn update_account(
    connections: &Connections,
    account_id: &AccountId,
    last_successful_update_nanos: Option<i64>,
) -> Result<(), UpdateAccountError> {
    let (score, balance) = join!(
        calculate_account_score(
            &connections.indexer_pool,
            account_id,
            last_successful_update_nanos
        ),
        get_account_balance(&connections.rpc_client, account_id)
    );

    if score.is_err() || balance.is_err() {
        if let Err(ref error) = score {
            warn!("Failed to calculate score for {account_id}: {error:?}");
        }
        if let Err(ref error) = balance {
            warn!("Failed to retrieve balance for {account_id}: {error:?}");
        }

        let err_query_result = sqlx::query!(
            r#"--sql
INSERT INTO account(id, consecutive_errors)
    VALUES ($1, 1)
    ON CONFLICT (id) DO
        UPDATE SET consecutive_errors = account.consecutive_errors + 1
        "#,
            account_id.to_string()
        )
        .execute(&connections.local_pool)
        .await;

        match err_query_result {
            Ok(..) => info!("Successfully updated consecutive_errors"),
            Err(e) => error!("Failed to update consecutive_errors: {e:?}"),
        };
    }

    let score = score.map_err(UpdateAccountError::Score)?;
    let balance = Decimal::from_u128(balance?);

    sqlx::query!(
        "--sql
INSERT INTO account(id, balance, score, consecutive_errors)
    VALUES ($1, $2, $3, 0)
    ON CONFLICT (id) DO
        UPDATE SET
            balance = EXCLUDED.balance,
            score = EXCLUDED.score,
            consecutive_errors = 0
",
        account_id.to_string(),
        balance,
        score as i64
    )
    .execute(&connections.local_pool)
    .await?;

    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn start_local_updater(
    connections: Arc<Connections>,
    mut input: mpsc::Receiver<(
        AccountId,
        Option<i64>,
        oneshot::Sender<Result<(), UpdateAccountError>>,
    )>,
) {
    tokio::spawn(async move {
        while let Some((account_id, last_successful_update_nanos, output)) = input.recv().await {
            let connections = connections.clone();

            tokio::spawn(async move {
                let result =
                    update_account(&connections, &account_id, last_successful_update_nanos).await;

                if let Err(_e) = output.send(result) {
                    error!("Error sending output oneshot channel");
                }
            });
        }
    });
}
