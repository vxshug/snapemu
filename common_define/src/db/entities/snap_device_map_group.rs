//! `SeaORM` Entity, @generated by sea-orm-codegen 1.0.1

use sea_orm::entity::prelude::*;
use crate::Id;
use crate::time::Timestamp;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "snap_device_map_group")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: Id,
    pub user_id: Id,
    pub device_id: Id,
    pub group_id: Id,
    pub dev_order: i32,
    pub create_time: Timestamp,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::snap_devices::Entity",
        from = "Column::DeviceId",
        to = "super::snap_devices::Column::Id"
    )]
    Device,
    #[sea_orm(
        belongs_to = "super::snap_device_group::Entity",
        from = "Column::GroupId",
        to = "super::snap_device_group::Column::Id"
    )]
    Group,
    #[sea_orm(has_many = "super::snap_device_group::Entity")]
    MoreGroup
}

impl Related<super::snap_device_group::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::MoreGroup.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
