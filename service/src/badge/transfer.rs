use std::{collections::HashSet, str::FromStr, sync::Arc};

use crate::badge::BadgeCheckResult;

use futures::FutureExt;
use lazy_static::lazy_static;
use near_primitives::types::AccountId;
use sqlx::types::Uuid;
use thiserror::Error;

use super::Connections;

// Badge UUIDs must correlate with badge renderers on the frontend

lazy_static! {
    pub static ref TRANSFER_1: Uuid =
        Uuid::from_str("b36c3dd2-b8b0-4098-b227-63290f009668").unwrap();
    pub static ref TRANSFER_10: Uuid =
        Uuid::from_str("609b2017-9534-4737-b86b-6ee4897fc4f9").unwrap();
    pub static ref TRANSFER_100: Uuid =
        Uuid::from_str("64ce9af9-52ac-49dc-96fd-031b4fa2efad").unwrap();
    pub static ref BADGE_IDS: HashSet<Uuid> =
        HashSet::from([*TRANSFER_1, *TRANSFER_10, *TRANSFER_100,]);
}

#[derive(Error, Debug)]
enum TransferBadgeError {
    #[error("Query failure: {0}")]
    Query(#[from] sqlx::Error),
}

async fn perform_query(
    connections: Arc<Connections>,
    account_id: AccountId,
) -> Result<BadgeCheckResult, TransferBadgeError> {
    #[derive(sqlx::FromRow)]
    struct WithResult {
        pub result: i64,
    }

    sqlx::query_as::<_, WithResult>(
        r#"--sql
select count(*) as result
    from action_receipt_actions
    where action_kind = 'TRANSFER'
        and receipt_predecessor_account_id = $1
"#,
    )
    .bind(account_id.to_string())
    .fetch_one(&connections.indexer_pool)
    .map(|result| {
        result
            .map(|r| {
                let total = r.result;
                let mut awarded = HashSet::new();

                if total >= 100 {
                    awarded.insert(TRANSFER_100.to_owned());
                }
                if total >= 10 {
                    awarded.insert(TRANSFER_10.to_owned());
                }
                if total >= 1 {
                    awarded.insert(TRANSFER_1.to_owned());
                }

                BadgeCheckResult {
                    account_id,
                    awarded,
                    checked: &BADGE_IDS,
                }
            })
            .map_err(TransferBadgeError::from)
    })
    .await
}

create_badge_worker!(perform_query);
