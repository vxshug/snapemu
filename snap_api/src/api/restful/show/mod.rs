mod device;

use axum::Router;
use crate::AppState;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .nest("/device", device::router())
}