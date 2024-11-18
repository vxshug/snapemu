use axum::{routing::get, Router};
use axum::extract::State;
use axum::routing::delete;
use sea_orm::TransactionTrait;
use tracing::instrument;
use common_define::Id;
use common_define::product::DeviceType;
use crate::api::{SnJson, SnPath};

use crate::service::device::device::{DeviceResp, DeviceInfo, DeviceSource, DeviceCreate, DeviceModify, MQTTDeviceInfo, DeviceWithAuth};

use crate::error::{ApiError, ApiResponseResult};
use crate::{AppString, get_current_user, tt, AppState, GLOBAL_PRODUCT_NAME};
use crate::cache::DeviceCache;
use crate::service::device::DeviceService;
use crate::service::device::group::{DeviceGroupResp, DeviceGroupService};
use crate::service::lorawan::{LoRaGateService, LoRaNodeService};
use crate::service::mqtt::MQTTService;
use crate::service::snap::SnapDeviceService;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(get_all_device).post(post_device))
        .route("/:id", delete(delete_device).get(get_device).put(put_device))
}


/// Get all devices
///
#[utoipa::path(
    get,
    path = "/device",
    responses(
            (status = 200, description = "List matching todos by query", body = [Todo])
    )
)]
async fn get_all_device(
    State(state): State<AppState>,
) -> ApiResponseResult<DeviceGroupResp> {
    let user = get_current_user();
    let redis = &mut state.redis.get().await?;
    let mut all = DeviceGroupService::select_default_group(&user, redis, &state).await?;
    all.id = None;
    all.name = None;
    all.default_group = None;
    all.description = None;
    Ok(all.into())
}

async fn get_device(
    State(state): State<AppState>,
    SnPath(id): SnPath<Id>,
) -> ApiResponseResult<DeviceResp> {
    let user = get_current_user();
    let conn = &state.db;
    let DeviceWithAuth { auth, device } = DeviceService::query_one_with_auth(user.id, id, conn).await?;
    let info = match device.device_type {
        DeviceType::MQTT => {
            let s = MQTTService::get(id, conn).await?;
            DeviceInfo::MQTT(MQTTDeviceInfo::new(id, s.eui, s.username, s.password))
        }
        DeviceType::LoRaNode => {
            let info = LoRaNodeService::get_lora_node(id, conn).await?;
            DeviceInfo::LoRaNode(info.into())
        }
        DeviceType::LoRaGate => {
            let info = LoRaGateService::get_gateway(id, conn).await?;
            DeviceInfo::LoRaGate(info.into())
        }
        DeviceType::Snap => {
            let info = SnapDeviceService::get_device(id, conn).await?;
            DeviceInfo::Snap(info.into())
        }
    };

    let group = DeviceGroupService::query_by_device(device.id, &user, conn).await?;
    let group = group.into_iter().map(|item| DeviceGroupResp {
        id: item.id.into(),
        name: item.name.into(),
        ..Default::default()
    }).collect();
    let product = GLOBAL_PRODUCT_NAME.get_by_id(device.product_id);
    let (product_id, product_url) = match product {
        Some(p) => (Some(p.id), Some(p.image)),
        None => (None, None)
    };
    let resp = DeviceResp {
        id: device.id.into(),
        name: device.name.into(),
        blue_name: None,
        online: device.online.into(),
        charge: None,
        battery: None,
        description: device.description.into(),
        info: info.into(),
        source: DeviceSource {
            share_type: auth.share_type,
            owner: auth.owner,
            manager: auth.manager,
            modify: auth.modify,
            delete: auth.delete,
            share: auth.share,
        }.into(),
        device_type: device.device_type.into(),
        product_type: None,
        create_time: device.create_time.into(),
        active_time: device.active_time.into(),
        data: None,
        script: device.script,
        product_id,
        product_url,
        group: Some(group),
    };
    Ok(resp.into())
}

#[instrument(skip(state))]
async fn post_device(
    State(state): State<AppState>,
    SnJson(req): SnJson<DeviceCreate>,
) -> ApiResponseResult<AppString> {
    let user = get_current_user();
    if req.eui.is_none() {
        return Err(ApiError::User(tt!("messages.device.common.eui_missing")))
    }
    
    let redis = state.redis.clone();
    
    state.db.transaction::<_, _, ApiError>(|ctx| {
        Box::pin(async move {
            let mut redis = redis.get().await?;
            DeviceService::new_device(&user, req, &mut redis, ctx).await?;
            DeviceCache::delete_by_user_id(user.id, &mut redis).await?;
            Ok(())
        })
    }).await?;
    
    Ok(tt!("messages.device.create_success").into())
}

async fn delete_device(
    State(state): State<AppState>,
    SnPath(id): SnPath<Id>,
) -> ApiResponseResult<AppString> {
    let redis = state.redis.clone();
    state.db.transaction::<_, _, ApiError>(|ctx| {
        Box::pin(async move {
            let user = get_current_user();
            let mut redis = redis.get().await?;
            DeviceService::delete(id, &user, &mut redis, ctx).await?;
            DeviceCache::delete_by_user_id(user.id, &mut redis).await?;
            Ok(())
        })
    }).await?;
    Ok(tt!("messages.device.delete_success").into())
}

async fn put_device(
    State(state): State<AppState>,
    SnPath(id): SnPath<Id>,
    SnJson(req): SnJson<DeviceModify>,
) -> ApiResponseResult<String> {
    let user = get_current_user();
    let device_with_auth = DeviceService::query_one_with_auth(user.id, id, &state.db).await?;
    let redis = state.redis.clone();
    state.db.transaction::<_, _, ApiError>(|ctx| {
        Box::pin(async move {
            let mut redis = redis.get().await?;
            DeviceService::update_info(device_with_auth, req, &mut redis, ctx).await?;
            DeviceCache::delete_by_user_id(user.id, &mut redis).await?;
            Ok(())
        })
    }).await?;

    Ok(String::new().into())
}