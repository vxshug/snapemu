use crate::AppState;
use axum::Router;

mod mqtt;

pub(crate) fn router() -> Router<AppState> {
    Router::new().nest("/mqtt", mqtt::router())
}
