use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use common_define::db::{DeviceDataColumn, DeviceDataEntity};
use common_define::Id;
use crate::error::ApiResult;
use crate::service::data::DataService;

impl DataService {

    pub(crate) async fn delete_by_device_id<C: ConnectionTrait>(device: Id, conn: &C) -> ApiResult {
        DeviceDataEntity::delete_many()
            .filter(DeviceDataColumn::DeviceId.eq(device))
            .exec(conn).await?;
        Ok(())
    }

    pub(crate) async fn delete_by_device_id_array<C: ConnectionTrait>(devices: &[Id], conn: &C) -> ApiResult {
        DeviceDataEntity::delete_many()
            .filter(DeviceDataColumn::DeviceId.is_in(devices))
            .exec(conn).await?;
        Ok(())
    }
}