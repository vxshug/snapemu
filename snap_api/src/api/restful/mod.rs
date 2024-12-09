use crate::{service, AppState};

use axum::{middleware, Router};
use axum::routing::{get, post};
use utoipa::{Modify, OpenApi};
use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
use utoipa_axum::router::OpenApiRouter;
use utoipa_scalar::{Scalar, Servable};
use crate::api::restful::device::register_query;
use crate::load::load_config;

pub(crate) mod data;
pub(crate) mod device;
pub(crate) mod user;
mod integration;
mod verify;
mod decode;
mod show;
mod app;
mod admin;
mod contact;


pub(crate) fn router() -> OpenApiRouter<AppState> {
    let api = OpenApiRouter::new()
        .nest("/data", data::router())
        // .nest("/integration", integration::router())
        .nest("/device", device::router())
        .nest("/decode", decode::router())
        // .nest("/show", show::router())
        .layer(middleware::from_fn(service::user::auth));
    let api = OpenApiRouter::new()
        .route("/contact", post(contact::contact_us))
        .route("/app/version", get(app::version))
        .route("/device/query/register", post(register_query))
        // .nest("/verify", verify::router())
        // .nest("/admin", admin::router())
        .nest("/user", user::router()).merge(api);
    // if config.api.openapi {
    //     Router::new().nest("/v1", api)
    //         .merge(Scalar::with_url("/scalar", ApiDoc::openapi()))
    // } else {
    //     Router::new().nest("/v1", api)
    // }
    api
}


struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "Authorization",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("Authorization"))),
            )
        }
    }
}