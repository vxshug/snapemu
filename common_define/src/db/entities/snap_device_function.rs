//! `SeaORM` Entity, @generated by sea-orm-codegen 1.0.1

use sea_orm::entity::prelude::*;
use crate::Id;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "snap_device_function")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub device: Id,
    #[sea_orm(column_type = "Text")]
    pub func_name: String,
    #[sea_orm(column_type = "Text")]
    pub func_value: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::snap_devices::Entity",
        from = "Column::Device",
        to = "super::snap_devices::Column::Id"
    )]
    Device,
}

impl Related<super::snap_devices::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Device.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
