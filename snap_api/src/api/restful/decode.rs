use axum::{Router};
use axum::extract::State;
use axum::routing::{delete, post};
use common_define::Id;
use crate::api::{SnJson, SnPath};
use crate::error::ApiResponseResult;
use crate::{get_current_user, AppState};
use crate::service::decode::{DecodeService, ScriptRequest};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(new_script).get(list_script))
        .route("/:id", delete(delete_script))
}

async fn new_script(
    State(state): State<AppState>,
    SnJson(req): SnJson<ScriptRequest>
) -> ApiResponseResult<ScriptRequest> {
    let user = get_current_user();
    let script = DecodeService::insert_script(
        &user,
        req,
        &state.db
    ).await?;
    Ok(script.into())
}
async fn list_script(
    State(state): State<AppState>,
) -> ApiResponseResult<Vec<ScriptRequest>> {
    let user = get_current_user();
    
    let s = DecodeService::list(
        &user,
        &state.db
    ).await?;
    Ok(s.into())
}


async fn delete_script(
    State(state): State<AppState>,
    SnPath(id): SnPath<Id>
) -> ApiResponseResult<String> {
    let user = get_current_user();
    
    DecodeService::delete_script(&user, id, &state.db).await?;
    Ok(String::new().into())
}