use crate::api::SnJson;
use crate::error::{ApiError, ApiResponseResult};
use crate::service::integration::mqtt::{IntegrationMqttReq, MqttToken, MqttTokenResp};
use crate::service::integration::IntegrationService;
use crate::{get_current_user, AppState};
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use sea_orm::TransactionTrait;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(query).post(register))
}

async fn register(
    State(state): State<AppState>,
    SnJson(req): SnJson<IntegrationMqttReq>,
) -> ApiResponseResult<MqttToken> {
    let token = state
        .db
        .transaction::<_, _, ApiError>(|ctx| {
            Box::pin(async move {
                let user = get_current_user();
                let token = IntegrationService::mqtt_register(&user, req, ctx).await?;
                Ok(token)
            })
        })
        .await?;

    Ok(token.into())
}

async fn query(State(state): State<AppState>) -> ApiResponseResult<MqttTokenResp> {
    let user = get_current_user();

    let resp = IntegrationService::query_all(&user, &state.db).await?;
    Ok(resp.into())
}
