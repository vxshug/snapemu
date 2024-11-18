use crate::{service, AppState};

use axum::{middleware, Router};
use axum::routing::{get, post};
use utoipa::{Modify, OpenApi};
use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
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


pub(crate) fn router() -> Router<AppState> {
    let config = load_config();
    let api = Router::new()
        .nest("/data", data::router())
        .nest("/integration", integration::router())
        .nest("/device", device::router())
        .nest("/decode", decode::router())
        .nest("/show", show::router())
        .layer(middleware::from_fn(service::user::auth));
    let api = Router::new()
        .route("/contact", post(contact::contact_us))
        .route("/app/version", get(app::version))
        .route("/device/query/register", post(register_query))
        .nest("/verify", verify::router())
        .nest("/admin", admin::router())
        .nest("/user", user::router()).merge(api);
    if config.api.openapi {
        Router::new().nest("/v1", api)
            .merge(Scalar::with_url("/scalar", ApiDoc::openapi()))
    } else {
        Router::new().nest("/v1", api)
    }

}

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    nest(
        (path = "/api/v1/user", api = user::UserApi),
        (path = "/api/v1/admin", api = admin::AdminApi),
    ),
    tags(
            (name = "Snapemu", description = "Snapemu API")
    ),
    components(schemas(
        crate::error::ApiStatus, 
        crate::error::ApiResponseEmpty,
        crate::error::ApiResponseToken,
        service::user::Token,
    ))
)]
struct ApiDoc;

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