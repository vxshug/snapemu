use sea_orm::entity::prelude::*;
use crate::Id;
use crate::time::Timestamp;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "snap_downlink")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: Id,
    pub device_id: Id,
    pub user_id: Id,
    #[sea_orm(column_type = "Text")]
    pub name: String,
    #[sea_orm(column_type = "Text")]
    pub data: String,
    pub order: i32,
    pub port: i32,
    pub create_time: Timestamp,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {

}

impl ActiveModelBehavior for ActiveModel {}
