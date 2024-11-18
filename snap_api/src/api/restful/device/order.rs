use axum::{Router};
use axum::extract::State;
use axum::routing::put;
use common_define::Id;
use crate::api::SnPath;
use crate::error::ApiResponseResult;
use crate::{get_current_user, AppState};
use crate::service::device::order::DeviceOrderService;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/top/:id", put(put_top))
        .route("/top/:id/:group", put(put_group_top))
}
async fn put_top(
    State(state): State<AppState>,
    SnPath(device): SnPath<Id>
) -> ApiResponseResult<String> {
    let user = get_current_user();
    let redis = &mut state.redis.get().await?;
    DeviceOrderService::device_top(&user, device, None, redis, &state.db).await?;
    Ok(String::from("OK").into())
}
async fn put_group_top(
    State(state): State<AppState>,
    SnPath((device, group)): SnPath<(Id, Id)>
) -> ApiResponseResult<String> {
    let user = get_current_user();
    let redis = &mut state.redis.get().await?;
    DeviceOrderService::device_top(&user, device, Some(group), redis, &state.db).await?;
    Ok(String::from("OK").into())
}
