use axum::extract::State;
use crate::error::ApiResponseResult;
use crate::service::user::{Token, TokenService};
use crate::api::SnJson;
use crate::AppState;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub(crate) struct Refresh {
    refresh: String,
}

pub(crate) async fn refresh(
    State(state): State<AppState>,
    SnJson(token): SnJson<Refresh>,
) -> ApiResponseResult<Token> {
    let token = TokenService::refresh_key(&token.refresh, &state).await?;
    Ok(token.into())
}
