use axum::routing::{post, put};
use axum::{middleware, Router};
use crate::{service, AppState};

pub(crate) mod login;
mod refresh;
mod signup;
mod info;
mod picture;
mod delete;

pub(super) mod _api {
    use utoipa::OpenApi;
    
    

    use crate::service;
    
    #[derive(OpenApi)]
    #[openapi(
        components(schemas(service::user::LoginUser)),
        tags((name = "user"))
    )]
    pub struct UserApi;
}

pub use _api::UserApi;

pub(crate) fn router() -> Router<AppState> {
    
    let picture = Router::new()
        .route("/picture", post(picture::picture).layer(middleware::from_fn(service::user::auth)))
        .layer(axum::extract::DefaultBodyLimit::max(5 * 1024 * 1024));
    Router::new()
        .route("/info", put(info::info).get(info::get_info).layer(middleware::from_fn(service::user::auth)))
        .route("/login", post(login::user_login))
        // .route("/verify/:token", get(login::verify_email))
        .route("/signup", post(signup::user_signup))
        .route("/reset", post(signup::reset))
        .route("/reset/password", post(signup::reset_password))
        .route("/refresh", post(refresh::refresh))
        // .nest("/auth", auth::router())
        .nest("/delete", delete::router())
        .merge(picture)
}


