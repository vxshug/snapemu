use axum::extract::State;
use crate::api::SnJson;
use crate::error::ApiResponseResult;
use crate::{get_current_user, AppState};
use crate::service::user::{UserPutInfo, UserRespInfo, UserService};



pub(crate) async fn info(
    State(state): State<AppState>,
    SnJson(info): SnJson<UserPutInfo>
) -> ApiResponseResult<String> {
    let user = get_current_user();
    UserService::info(&user, info, &state).await?;
    Ok(String::new().into())
}

pub(crate) async fn get_info(
    State(state): State<AppState>,
) -> ApiResponseResult<UserRespInfo> {
    let user = get_current_user();
    
    Ok(UserService::get_info(&user, &state).await?.into())
}
