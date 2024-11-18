use axum::{Extension, Router};
use axum::extract::State;
use axum::response::{IntoResponse, Response, Sse};
use axum::response::sse::{Event, KeepAlive};
use axum::routing::get;
use tokio_stream::StreamExt;
use tracing::warn;
use common_define::Id;
use crate::api::SnPath;
use crate::error::ApiError;
use crate::{get_current_user, tt, AppState};
use crate::man::NodeEventManager;
use crate::service::device::DeviceService;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/:id", get(log))
}

async fn log(
    SnPath(device): SnPath<Id>,
    State(state): State<AppState>,
    Extension(mg): Extension<NodeEventManager>
) -> Result< Response, ApiError> {
    let user = get_current_user();
    DeviceService::query_one(user.id, device, &state.db).await?;
    let event = mg.subscribe(device);
    let s = event.
        into_stream()
        .map(|event| match event {
            Ok(e) => Event::default().json_data(e).map_err(| e| {
                warn!(
                    "{}",
                    e
                );
                ApiError::User(tt!("messages.device.log.stream_decode"))
            }),
            Err(e) => {
                warn!(
                    "{}",
                    e
                );
                
                Err(ApiError::User(tt!("messages.device.log.stream_err")))
            }
        });
    let mut response = Sse::new(s).keep_alive(KeepAlive::default()).into_response();
    response.headers_mut().insert("X-Accel-Buffering", "no".parse().unwrap()); // nginx
    Ok(response)
}