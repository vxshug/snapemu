use crate::error::{ApiError, ApiResponseResult};
use crate::service::device::group::{
    DeviceGroupService, DeviceGroupResp, ReqDeviceGroup, ReqPutDeviceGroup
};
use axum::routing::{delete, post};
use axum::{Json, Router};
use axum::extract::State;
use sea_orm::TransactionTrait;
use common_define::Id;
use crate::api::{SnJson, SnPath};
use crate::{AppString, get_current_user, tt, AppState};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(create).get(all_group))
        .route("/:id", delete(delete_group).put(put_group).get(get_group))
        .route("/:id/:device", delete(delete_group_device))
}

async fn create(
    State(app_state): State<AppState>,
    group: Json<ReqDeviceGroup>,
) -> ApiResponseResult<AppString> {
    let user = get_current_user();
    let redis = &mut app_state.redis.get().await?;
    DeviceGroupService::create_group(group.0, &user, redis, &app_state.db).await?;
    Ok(tt!("messages.device.group.create_success").into())
}

async fn get_group(
    State(state): State<AppState>,
    SnPath(id): SnPath<Id>,
) -> ApiResponseResult<DeviceGroupResp> {
    let user = get_current_user();
    let mut redis = state.redis.get().await?;
    let group = DeviceGroupService::select_by_group_id(&user, id, &mut redis, &state.db).await?;
    let groups = DeviceGroupService::select_one(&user, &state, group).await?;
    Ok(groups.into())
}

async fn all_group(
    State(state): State<AppState>,
) -> ApiResponseResult<Vec<DeviceGroupResp>> {
    let user = get_current_user();
    
    let groups = DeviceGroupService::select_all(&user, &state.db).await?;
    Ok(groups.into())
}

async fn delete_group(
    State(state): State<AppState>,
    SnPath(id): SnPath<Id>,
) -> ApiResponseResult<AppString> {
    let mut redis = state.redis.get().await?;
    state.db.transaction::<_,_, ApiError>(|ctx| {
        Box::pin(async move {
            let user = get_current_user();
            DeviceGroupService::delete_one(&user, id, &mut redis, ctx).await?;
            Ok(())
        })
    }).await?;
    Ok(tt!("messages.device.group.delete_success").into())
}

async fn delete_group_device(
    State(state): State<AppState>,
    SnPath((group, device)): SnPath<(Id, Id)>,
) -> ApiResponseResult<AppString> {
    let mut redis = state.redis.get().await?;
    let user = get_current_user();
    DeviceGroupService::unlink(device, user.id, group, &mut redis, &state.db).await?;
    Ok(tt!("messages.device.group.delete_device").into())
}


async fn put_group(
    State(state): State<AppState>,
    SnPath(group): SnPath<Id>,
    SnJson(req): SnJson<ReqPutDeviceGroup>,
) -> ApiResponseResult<AppString> {
    let mut redis = state.redis.get().await?;
    state.db.transaction::<_, _, ApiError>(|ctx| {
        Box::pin(async move {
            let user = get_current_user();
            DeviceGroupService::update_group(group, &user, req, &mut redis, ctx).await?;
            Ok(())
        })
    }).await?;
    Ok(tt!("messages.device.group.link_device").into())
}
