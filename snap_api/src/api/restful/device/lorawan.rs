
use axum::Router;

pub(crate) fn router<T: Clone + Send + Sync + 'static>() -> Router<T> {
    Router::new()
        // .route("/node",post(post_node))
        // .route("/gate", get(get_gate))
}

