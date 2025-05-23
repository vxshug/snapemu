//! `SeaORM` Entity, @generated by sea-orm-codegen 1.0.1

use crate::time::Timestamp;
use crate::Id;
use sea_orm::entity::prelude::*;
use crate::product::MqttType;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "snap_integration_mqtt")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: Id,
    pub user_id: Id,
    #[sea_orm(column_type = "Text")]
    pub mqtt_type: MqttType,
    #[sea_orm(column_type = "Text")]
    pub name: String,
    #[sea_orm(column_type = "Text")]
    pub username: String,
    #[sea_orm(column_type = "Text")]
    pub password: String,
    pub enable: bool,
    pub create_time: Timestamp,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
