
use crate::error::{ApiError, ApiResult};
use crate::{CurrentUser, tt};
use sea_orm::{ActiveModelTrait, ActiveValue, QueryFilter};
use sea_orm::ColumnTrait;

use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, EntityTrait};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use uuid::Uuid;
use common_define::db::{DeviceLoraGateActiveModel, DeviceLoraGateColumn, DeviceLoraGateEntity, DeviceLoraGateModel, Eui};
use common_define::Id;
use common_define::lora::LoRaRegion;
use common_define::product::DeviceType;
use common_define::time::Timestamp;
use device_info::lorawan::GatewayInfo;
use crate::man::DeviceQueryClient;
use crate::service::device::define::DeviceParameter;
use crate::service::device::DeviceService;

pub(crate) struct LoRaGateService;

#[derive(Deserialize, Serialize)]
pub(crate) struct Gate {
    eui: String,
    enable: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct LoraGatewayRes {
    pub(crate) id: Uuid,
    pub(crate) gateway_eui: String,
    pub(crate) active: bool,
    pub(crate) create_time: DateTime<Utc>,
    pub(crate) active_time: DateTime<Utc>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct ReqLoraGateway {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) eui: Eui,
    pub(crate) region: LoRaRegion,
}

impl LoRaGateService {
    #[instrument(skip_all)]
    pub(crate) async fn create<C: ConnectionTrait, R: redis::aio::ConnectionLike>(req: ReqLoraGateway, user: &CurrentUser, redis: &mut R, conn: &C) -> ApiResult<DeviceLoraGateModel> {
        DeviceService::valid_eui(req.eui, conn).await?;
        let eui = req.eui;
        
        let r = DeviceQueryClient::query_eui(eui.to_string().as_str()).await?;
        
        if let Some(g) = r {
            if g.device_type == DeviceType::LoRaGate {
                match g.parameter {
                    DeviceParameter::Device(_) => {
                        return Err(ApiError::User(
                            tt!("messages.device.lora.gate_eui_format")
                        ))
                    }
                    DeviceParameter::Gate(g) => {
                        if g.region != req.region {
                            return Err(ApiError::User(
                                tt!("messages.device.lora.gate_eui_format")
                            ))
                        }
                    }
                }
            }
        }
        
        let id = eui;
        

        let device = DeviceService::register_device(
            user,
            id,
            req.name.as_str(),
            req.description.as_str(),
            DeviceType::LoRaGate,
            conn
        ).await?;
        let gate = DeviceLoraGateActiveModel {
            id: Default::default(),
            device_id: ActiveValue::Set(device.id),
            region: ActiveValue::Set(req.region),
            eui: ActiveValue::Set(req.eui),
        };
        let gate = gate.insert(conn).await?;

        GatewayInfo::new(device.id, 0, 0, Timestamp::now(), None, None)
            .register(eui, redis)
            .await?;
        Ok(gate)
    }

    #[instrument(skip_all)]
    pub(crate) async fn delete_gateway<C: ConnectionTrait, R: redis::aio::ConnectionLike>(
        device_id: Id,
        redis: &mut R,
        conn: &C,
    ) -> ApiResult {
        let gate = DeviceLoraGateEntity::find()
            .filter(DeviceLoraGateColumn::DeviceId.eq(device_id))
            .one(conn)
            .await?;

        match gate {
            None => {
                return Err(ApiError::Device {
                    device_id,
                    msg: tt!("messages.device.lora.gate_missing")
                })
            }
            Some(gate) => {
                GatewayInfo::unregister(gate.eui, redis).await?;
            }
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub(crate) async fn get_gateway<C: ConnectionTrait>(device_id: Id, conn: &C) -> ApiResult<DeviceLoraGateModel> {
        DeviceLoraGateEntity::find()
            .filter(DeviceLoraGateColumn::DeviceId.eq(device_id))
            .one(conn)
            .await?
            .ok_or(ApiError::Device{ device_id, msg: tt!("messages.device.common.device_missing", device_id=device_id) })
    }
}
