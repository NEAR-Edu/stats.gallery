use near_jsonrpc_client::{
    errors::JsonRpcError,
    methods::{self, query::RpcQueryError},
    JsonRpcClient,
};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::{
    types::{AccountId, BlockReference, Finality},
    views::QueryRequest,
};

#[tracing::instrument(skip(client))]
pub async fn get_account_balance(
    client: &JsonRpcClient,
    account_id: &AccountId,
) -> Result<u128, JsonRpcError<RpcQueryError>> {
    client
        .call(methods::query::RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::Final),
            request: QueryRequest::ViewAccount {
                account_id: account_id.clone(),
            },
        })
        .await
        .map(|r| match r.kind {
            QueryResponseKind::ViewAccount(view_account) => view_account.amount,
            _ => 0, // unlikely unless RPC malfunction
        })
}
