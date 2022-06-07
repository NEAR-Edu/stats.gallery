//! SeaORM Entity. Generated by sea-orm-codegen 0.8.0

use super::sea_orm_active_enums::AccessKeyPermissionKind;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "access_keys")]
pub struct Model {
    #[sea_orm(primary_key, column_type = "Text")]
    pub public_key: String,
    #[sea_orm(primary_key, column_type = "Text")]
    pub account_id: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub created_by_receipt_id: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub deleted_by_receipt_id: Option<String>,
    pub permission_kind: AccessKeyPermissionKind,
    #[sea_orm(column_type = "Decimal(Some((20, 0)))")]
    pub last_update_block_height: Decimal,
}

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No RelationDef")
    }
}

impl ActiveModelBehavior for ActiveModel {}