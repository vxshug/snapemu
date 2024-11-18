use sea_orm::entity::prelude::*;
use crate::Id;
use crate::time::Timestamp;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "snap_config")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: Id,
    #[sea_orm(unique)]
    pub name: String,
    pub value: String,
    pub create_time: Timestamp,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {

}

impl ActiveModelBehavior for ActiveModel {}
