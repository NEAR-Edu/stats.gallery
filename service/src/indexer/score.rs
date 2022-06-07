use sqlx::FromRow;

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "action_kind")]
#[sqlx(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActionKind {
    CreateAccount,
    DeleteAccount,
    AddKey,
    DeleteKey,
    Transfer,
    Stake,
    DeployContract,
    FunctionCall,
}

#[derive(FromRow)]
pub struct ActionCountRow {
    pub action_kind: ActionKind,
    pub transaction_count: i64,
}

impl ActionCountRow {
    pub fn score_value(&self) -> u32 {
        self.transaction_count as u32
            * match &self.action_kind {
                ActionKind::CreateAccount => 50,
                ActionKind::AddKey => 1,
                ActionKind::DeleteKey => 25,
                ActionKind::Transfer => 10,
                ActionKind::DeployContract => 100,
                ActionKind::FunctionCall => 10,
                _ => 0,
            }
    }
}
