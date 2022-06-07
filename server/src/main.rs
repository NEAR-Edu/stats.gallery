use std::sync::Arc;

use actix_web::{
    get, http::StatusCode, web, App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use dotenv::dotenv;
use indexer::prelude::*;
use local::prelude::*;
use sea_orm::{
    ColumnTrait, Condition, Database, DatabaseConnection, EntityTrait, JoinType, QueryFilter,
    QueryOrder, QuerySelect, FromQueryResult,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod indexer;
mod local;

#[inline]
fn default_port() -> u16 {
    3000
}

#[derive(Deserialize)]
struct Configuration {
    #[serde(default = "default_port")]
    pub port: u16,
    pub database_url: String,
    pub indexer_url: String,
}

#[derive(Serialize)]
struct AppResponse<T: Serialize> {
    pub result: T,
}

#[derive(Serialize, Error, Debug)]
#[serde(tag = "error", rename_all = "snake_case")]
enum AppError {
    #[error("Database query error: {0:?}")]
    Query(
        #[from]
        #[serde(skip)] // Don't expose database errors externallysea_orm::error::DbErr,
        sea_orm::DbErr,
    ),
    #[error("Account not indexed: {account_id}")]
    NotFound { account_id: String },
}

impl actix_web::error::ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::NotFound { .. } => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let serialized = serde_json::to_string(self);
        match serialized {
            Ok(serialized) => HttpResponse::build(self.status_code())
                .content_type(mime::APPLICATION_JSON)
                .body(serialized),
            Err(_) => HttpResponse::InternalServerError()
                .content_type(mime::APPLICATION_JSON)
                .body("error"),
        }
    }
}

struct AppState {
    pub local_pool: Arc<DatabaseConnection>,
    pub indexer_pool: Arc<DatabaseConnection>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            local_pool: Arc::clone(&self.local_pool),
            indexer_pool: Arc::clone(&self.indexer_pool),
        }
    }
}

#[get("/status")]
async fn status() -> impl Responder {
    "ok"
}

#[derive(Deserialize)]
struct TimestampRange {
    pub after_block_timestamp: Option<u64>,
    pub before_block_timestamp: Option<u64>,
}

#[get("account/{account_id}/actions")]
async fn account_actions(
    state: web::Data<AppState>,
    req: HttpRequest,
    account_id: web::Path<String>,
) -> Result<impl Responder, AppError> {
    use indexer::*;

    #[derive(FromQueryResult, Serialize, Debug)]
    struct AccountAction {
      pub receipt_id: String,
      pub index_in_action_receipt: i32,
      pub transaction_hash: String,
      pub action_kind: String, // TODO: Enum?
      pub block_hash: String,
      pub block_timestamp: u64,
      pub predecessor_account_id: String,
      pub receiver_account_id: String,
    }

    let account_id = account_id.into_inner();
    let range = web::Query::<TimestampRange>::from_query(req.query_string()).unwrap(); // Should always be safe because req.query_string() should never return an invalid query string

    let condition = {
        let mut condition = Condition::all()
            .add(receipts::Column::ReceiptKind.eq("ACTION"))
            .add(
                Condition::any()
                    .add(receipts::Column::PredecessorAccountId.eq(account_id.clone()))
                    .add(receipts::Column::ReceiverAccountId.eq(account_id.clone())),
            );

        if let Some(after) = range.after_block_timestamp {
            condition = condition.add(receipts::Column::IncludedInBlockTimestamp.gte(after));
        }

        if let Some(before) = range.before_block_timestamp {
            condition = condition.add(receipts::Column::IncludedInBlockTimestamp.lt(before));
        }

        condition
    };

    let res = Receipts::find()
        .column_as(
            receipts::Column::OriginatedFromTransactionHash,
            "transaction_hash",
        )
        .column_as(receipts::Column::IncludedInBlockHash, "block_hash")
        .column_as(
            receipts::Column::IncludedInBlockTimestamp,
            "block_timestamp",
        )
        .column(action_receipt_actions::Column::IndexInActionReceipt)
        .column(action_receipt_actions::Column::ActionKind)
        .filter(condition)
        .join_rev(
            JoinType::LeftJoin,
            action_receipt_actions::Entity::belongs_to(receipts::Entity)
                .from(action_receipt_actions::Column::ReceiptId)
                .to(receipts::Column::ReceiptId)
                .into(),
        )
        .order_by_desc(receipts::Column::IncludedInBlockTimestamp)
        .limit(2000)
        .into_model::<AccountAction>()
        .all(&*state.indexer_pool)
        .await;

    if let Err(ref e) = res {
        println!("{e:?}");
    }
    
    let res = res?;

    Ok(web::Json(res))
}

#[get("account/{account_id}/score")]
async fn account_score(
    state: web::Data<AppState>,
    account_id: web::Path<String>,
) -> Result<impl Responder, AppError> {
    let account_id = account_id.into_inner();
    let res = Account::find_by_id(account_id.clone())
        .one(&*state.local_pool)
        .await?;

    if let Some(res) = res {
        Ok(web::Json(AppResponse { result: res.score }))
    } else {
        Err(AppError::NotFound { account_id })
    }
}

#[actix_web::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let config = envy::from_env::<Configuration>().unwrap();

    let local_pool = Database::connect(&config.database_url).await?;
    let indexer_pool = Database::connect(&config.indexer_url).await?;

    let state = AppState {
        local_pool: Arc::new(local_pool),
        indexer_pool: Arc::new(indexer_pool),
    };

    Ok(HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .service(status)
            .service(account_score)
            .service(account_actions)
    })
    .bind(("127.0.0.1", config.port))?
    .run()
    .await?)
}
