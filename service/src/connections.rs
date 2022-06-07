use near_jsonrpc_client::JsonRpcClient;
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct Connections {
    pub local_pool: PgPool,
    pub indexer_pool: PgPool,
    pub rpc_client: JsonRpcClient,
}
