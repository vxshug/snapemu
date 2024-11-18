use axum::Router;
use crate::AppState;

mod mqtt;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .nest("/mqtt", mqtt::router())
}
