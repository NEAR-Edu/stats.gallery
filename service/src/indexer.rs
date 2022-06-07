use async_recursion::async_recursion;
use chrono::TimeZone;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use near_primitives::types::AccountId;
use sqlx::PgPool;
use tap::Tap;
use thiserror::Error;
use tokio::join;
use tracing::{debug, error, info};

use crate::indexer::score::ActionCountRow;

mod score;

const FIRST_BLOCK_TIMESTAMP: i64 = 1595368210762782796;
// const FIRST_BLOCK_HEIGHT: i32 = 9820214;
const ONE_DAY_NANOS: i64 = 1000000000 * 60 * 60 * 24;
const MINIMUM_CHECKABLE_DURATION_NANOS: i64 = ONE_DAY_NANOS / 2;

#[tracing::instrument(skip(indexer_pool))]
pub async fn get_recent_actors(
    indexer_pool: &PgPool,
    timestamp_nanoseconds: u64,
) -> Result<Vec<String>, sqlx::Error> {
    #[derive(sqlx::FromRow)]
    struct AccountId {
        pub account_id: String,
    }

    sqlx::query_as::<_, AccountId>(
        r#"--sql
select distinct predecessor_account_id as account_id
    from receipts
    where included_in_block_timestamp > $1
        and length(predecessor_account_id) != 64
        and predecessor_account_id like '%.%'
union
select distinct receiver_account_id as account_id
    from receipts
    where included_in_block_timestamp > $1
        and length(receiver_account_id) != 64
        and receiver_account_id like '%.%'
"#,
    )
    .bind(timestamp_nanoseconds as i64) // for some reason Encode is not implemented for u64 on Postgres
    .fetch(indexer_pool)
    .map(|i| i.map(|a| a.account_id))
    .try_collect::<Vec<_>>()
    .await
}

#[derive(Error, Debug)]
pub enum ScoreCalculationError {
    #[error("Query error in calculating score: {0:?}")]
    SqlError(#[from] sqlx::Error),
    #[error("Over-recursion in calculating score")]
    OverRecursionError,
}

#[tracing::instrument(skip(indexer_pool))]
pub async fn calculate_account_score(
    indexer_pool: &PgPool,
    account_id: &AccountId,
    from_timestamp: Option<i64>,
) -> Result<u32, ScoreCalculationError> {
    calculate_account_score_rec(
        indexer_pool,
        account_id,
        from_timestamp.unwrap_or(FIRST_BLOCK_TIMESTAMP),
        chrono::Utc::now().timestamp_nanos(),
    )
    .await
}

#[async_recursion]
pub async fn calculate_account_score_rec(
    indexer_pool: &PgPool,
    account_id: &AccountId,
    min_timestamp: i64,
    max_timestamp: i64,
) -> Result<u32, ScoreCalculationError> {
    if max_timestamp - min_timestamp < MINIMUM_CHECKABLE_DURATION_NANOS {
        let min = chrono::Utc.timestamp_nanos(min_timestamp);
        let max = chrono::Utc.timestamp_nanos(max_timestamp);
        debug!("Over-recursion: {min} <> {max}");
        return Err(ScoreCalculationError::OverRecursionError);
    }

    Ok(sqlx::query_as::<_, ActionCountRow>(
        r#"--sql
select action_kind,
    count(*) as transaction_count
from transactions
left outer join transaction_actions on transactions.transaction_hash = transaction_actions.transaction_hash
where transactions.signer_account_id = $1
    and transactions.block_timestamp >= $2
    and transactions.block_timestamp < $3
group by action_kind
"#,
    )
    .bind(account_id.to_string())
    .bind(min_timestamp)
    .bind(max_timestamp)
    .fetch_all(indexer_pool)
    .map_ok(|v| v.iter().map(|r| r.score_value()).sum())
    .or_else(|e| async move {
        error!("Splitting because of error: {e:?}");
        let midpoint = (max_timestamp + min_timestamp) / 2;
        info!("Score split for {account_id}: {min_timestamp} | {max_timestamp}");
        let (first, second) = join!(
            calculate_account_score_rec(indexer_pool, account_id, min_timestamp, midpoint),
            calculate_account_score_rec(indexer_pool, account_id, midpoint, max_timestamp),
        );

        match (first, second) {
            (Ok(a), Ok(b)) => Ok(a + b),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    })
    .await?)
    .tap(|r| info!("Score result: {r:?}"))
}
