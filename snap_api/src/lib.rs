#![allow(dead_code)]

use crate::api::restful;
use async_graphql::http::GraphiQLSource;

use axum::http::{HeaderName, HeaderValue, Method, Request};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};

use axum::{middleware, Extension, Router, http};
use deadpool::managed::PoolConfig;
use futures_util::StreamExt;
use once_cell::sync::Lazy;
use sea_orm::EntityTrait;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use utoipa::{Modify, OpenApi};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa_axum::router::OpenApiRouter;
use utoipa_scalar::{Scalar, Servable};
use common_define::db::{SnapProductInfoEntity};
use common_define::event::DeviceEvent;
use snap_config::SnapConfig;



pub(crate) mod api;
pub(crate) mod error;
pub(crate) mod man;
pub(crate) mod service;
pub(crate) mod sub;
pub(crate) mod utils;
pub(crate) mod cache;
mod load;
mod local_key;
pub use local_key::*;
use migration::MigratorTrait;
use crate::cache::ProductNameCache;
use crate::load::{load_config, load_db, store_config};
use crate::man::{NodeEventManager, RedisClient, RedisRecv};
use crate::service::user::UserLang;


const SEA_ORMDB_BACKEND: sea_orm::DbBackend = sea_orm::DbBackend::Postgres;

const DEVICE_DATA_RAW_SQL: &str = r"SELECT id,device_id, data, bytes, create_time
FROM (
    SELECT id, device_id, data, bytes, create_time,
           ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY create_time DESC) AS row_num
    FROM snap_device_data
    WHERE device_id IN (parameter)
) AS subquery
WHERE row_num <= 2;";

static GLOBAL_PRODUCT_NAME: Lazy<ProductNameCache> = Lazy::new(|| {
    ProductNameCache::default()
});

static MODEL_MAP: Lazy<snap_model::ModelMap> = Lazy::new(|| {
    let config = load_config();
    snap_model::load_model_file(config.api.model.as_ref().map(|p| p.path.as_str() ))
});


#[derive(Clone)]
struct AppState {
    db: sea_orm::DatabaseConnection,
    redis: RedisClient
}

const USER_TAG: &str = "user";
const DEVICE_TAG: &str = "device";
const ADMIN_TAG: &str = "admin";
const DATA_TAG: &str = "data";
const DECODE_TAG: &str = "decode";

struct AdminSecurityAddon;

impl Modify for AdminSecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "Authorization",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            )
        }
    }
}

#[derive(utoipa::OpenApi)]
#[openapi(
    tags(
        (name = USER_TAG, description = "User API endpoints"),
        (name = DEVICE_TAG, description = "Device API endpoints"),
        (name = DATA_TAG, description = "Data API endpoints"),
        (name = DECODE_TAG, description = "Decode API endpoints"),
        (name = ADMIN_TAG, description = "Admin API endpoints")
    ),
    security(
        ("Authorization" = []),
    ),
    modifiers(&AdminSecurityAddon),
)]
struct ApiDoc;

snap_i18n::i18n!("locales");
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! tt {
    ($($all:tt)*) =>  {
        crate::_my_translation!($($all)*)
    }
}
type AppString = std::borrow::Cow<'static, str>;
type RedisPool = deadpool_redis::Pool;
type RedisConnection = deadpool_redis::Connection;

fn locale() -> &'static str {
    get_lang()
        .as_static_str()
}

fn load_redis(config: &SnapConfig) -> deadpool_redis::Pool  {
    let host = config.get_string("redis.host").unwrap();
    let port = config.get_i64("redis.port").unwrap_or(6379);
    let db = config.get_i64("redis.db").unwrap_or(0);
    let username = config.get_string("redis.username").unwrap_or_default();
    let password = config.get_string("redis.password").unwrap_or_default();
    let url = format!("redis://{}:{}@{}:{}/{}", username, password, host, port, db);
    let mut cfg = deadpool_redis::Config::from_url(url);
    cfg.pool = Some(PoolConfig::new(100));
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)).unwrap()
}

pub async fn run(config_path: String, env_prefix: String) {
    let config = store_config(config_path, env_prefix);
    snap_config::init_logging(config.log);
    let redis_pool = RedisClient::get_client();
    let db = load_db().await;

    migration::Migrator::up(&db, None).await.unwrap();
    let redis: RedisClient = RedisClient::get_client();
    let mut consumer = RedisRecv::new(redis.get_pubsub().await.unwrap());
    consumer.subscribe(DeviceEvent::KAFKA_TOPIC).await.unwrap();
    let event = NodeEventManager::new();
    let event1 = event.clone();

    tokio::spawn(async move {
        loop {
            let mut message = consumer.message();
            while let Some(msg) = message.next().await {
                match serde_json::from_slice::<DeviceEvent>(msg.get_payload_bytes()) {
                    Ok(e) => {
                        event1.broadcast(e)
                    }
                    Err(e) => {
                        warn!("{}", e)
                    }
                }
            }
        }
    });
    let state = AppState {
        db,
        redis: redis_pool.clone()
    };
    let products = SnapProductInfoEntity::find().all(&state.db).await.unwrap();

    GLOBAL_PRODUCT_NAME.replace(products);

    let fs = Router::new().nest_service("/", ServeDir::new("assets"));
    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .nest("/api/v1", restful::router())
        .split_for_parts();
    let router = if config.api.openapi {
        router
            .merge(Scalar::with_url("/api/scalar", api))
    } else { router };
    let app = router
        .nest("/assets", fs)
        .layer(Extension(redis_pool))
        .layer(Extension(event))
        .layer(middleware::from_fn(accept_language))
        .with_state(state);
    let app = if config.api.tracing {
        let trace = TraceLayer::new_for_http();
        app.layer(trace)
    } else { app };

    let app = if config.api.cors {
        app.layer(
            CorsLayer::new()
                .allow_origin("*".parse::<HeaderValue>().unwrap())
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_headers(["Authorization".parse::<HeaderName>().unwrap(), "*".parse::<HeaderName>().unwrap()])
        )
    } else { app };

    let url = {
        let api_host = config.api.host.clone();
        let api_post = config.api.port;
        format!("{}:{}", api_host, api_post)
    };
    info!("listen: {}", url);
    info!("web url: {}", config.api.web_url);
    let listener = tokio::net::TcpListener::bind(url).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn accept_language(request: Request<axum::body::Body>, next: Next) -> Response {
    let lang = request.headers()
        .get(http::header::ACCEPT_LANGUAGE)
        .and_then(|header| header.to_str().ok());
    let lang = UserLang::form_str(lang);
    run_with_lang(lang, next.run(request)).await
}


