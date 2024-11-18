use sea_orm::{ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, EntityTrait, ModelTrait, QueryFilter};
use common_define::db::{SnapIntegrationMqttActiveModel, SnapIntegrationMqttColumn, SnapIntegrationMqttEntity, SnapIntegrationMqttModel};
use common_define::Id;
use common_define::time::Timestamp;
use crate::{CurrentUser, tt};
use crate::error::{ApiError, ApiResult};
use crate::service::device::DeviceService;
use crate::service::integration::IntegrationService;
use crate::utils::Rand;

#[derive(serde::Deserialize)]
pub struct IntegrationMqttReq {
    group: Option<bool>,
    name: String,
    device: Id
}

#[derive(serde::Serialize)]
pub struct MqttToken {
    name: String,
    enable: bool,
    username: String,
    password: String,
    client_id: String,
    create_time: Timestamp,
}

#[derive(serde::Serialize)]
pub struct MqttTokenResp {
    count: usize,
    tokens: Vec<MqttToken>
}

#[derive(serde::Serialize, serde::Deserialize, Copy, Clone, Debug, strum::AsRefStr, strum::EnumString)]
pub enum ShareType {
    Device,
    Group
}

impl IntegrationService {
    pub async fn mqtt_register<C: ConnectionTrait>(
        user: &CurrentUser,
        device: IntegrationMqttReq,
        conn: &C
    ) -> ApiResult<MqttToken> {
        let share_type = match device.group {
            None => {
                DeviceService::query_one(user.id, device.device, conn).await?;
                ShareType::Device
            }
            Some(f) => {
                if f { ShareType::Group } else {
                    DeviceService::query_one(user.id, device.device, conn).await?;
                    ShareType::Device
                }
            }
        };

        let device_hex = device.device.to_string();
        let user_hex = user.id.to_string();
        let rand = Rand::string(17);
        let token = format!("SNAP.{}.{}.{}", device_hex, user_hex, rand);
        let now = Timestamp::now();

        let model = SnapIntegrationMqttActiveModel {
            id: Default::default(),
            user_id: ActiveValue::Set(user.id),
            share: ActiveValue::Set(user.id),
            share_type: ActiveValue::Set(share_type.as_ref().to_string()),
            name: ActiveValue::Set(device.name),
            enable: ActiveValue::Set(true),
            token: ActiveValue::Set(token),
            create_time: ActiveValue::Set(now),
        };
        let model = model.insert(conn).await?;

        Ok(MqttToken {
            name: model.name,
            enable: true,
            username: device.device.to_string(),
            password: model.token,
            client_id: format!("{}@{}@[index]", device.device, share_type.as_ref().to_ascii_lowercase()),
            create_time: now,
        })
    }

    pub async fn query_all<C: ConnectionTrait>(
        user: &CurrentUser,
        conn: &C
    ) -> ApiResult<MqttTokenResp> {
        let tokens = SnapIntegrationMqttEntity::find()
            .filter(SnapIntegrationMqttColumn::UserId.eq(user.id))
            .all(conn)
            .await?;
        let tokens: Vec<MqttToken> = tokens.into_iter().map(|item| {
            MqttToken {
                name: item.name,
                enable: item.enable,
                username: item.share.to_string(),
                password: item.token,
                client_id: format!("{}@{}@[index]", item.share, item.share_type.as_str().to_ascii_lowercase()),
                create_time: item.create_time,
            }
        }).collect();
        Ok(MqttTokenResp {
            count: tokens.len(),
            tokens,
        })
    }

    pub async fn query_one<C: ConnectionTrait>(
        token: &str,
        conn: &C
    ) -> ApiResult<SnapIntegrationMqttModel> {

        let token = SnapIntegrationMqttEntity::find()
            .filter(SnapIntegrationMqttColumn::Token.eq(token))
            .one(conn)
            .await?;

        match token {
            Some(token) => Ok(token),
            None => Err(ApiError::User(
               tt!("messages.device.mqtt.not_found_device")
            ))
        }
    }

    pub async fn delete<C: ConnectionTrait>(
        user: &CurrentUser,
        id: Id,
        conn: &C
    ) -> ApiResult {
        let token = SnapIntegrationMqttEntity::find_by_id(id).one(conn).await?;
        if let Some(token) = token {
            if token.user_id == user.id {
                token.delete(conn).await?;
            }
        }
        Ok(())
    }
}