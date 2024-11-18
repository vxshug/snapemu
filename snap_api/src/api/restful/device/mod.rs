use axum::Router;
use utoipa::OpenApi;

mod devices;
mod group;
mod lorawan;
mod order;
mod query;
pub use query::register_query;
use crate::AppState;

mod io;
mod log;
mod down;
mod map;
mod product;
// mod model;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
       .nest("/order", order::router())
        .nest("/group", group::router())
        .nest("/device", devices::router())
        .nest("/down", down::router())
        .nest("/map", map::router())
        .nest("/io", io::router())
        .nest("/log", log::router())
        .nest("/query", query::router())
        .nest("/lorawan", lorawan::router())
        .nest("/product", product::router())
}

#[derive(OpenApi)]
#[openapi(
    paths(devices::get_all_device),
)]
pub(super) struct DeviceApi;