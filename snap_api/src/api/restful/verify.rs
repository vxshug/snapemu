use axum::{Json, Router};
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use crate::api::SnJson;
use crate::AppState;
use crate::service::integration::IntegrationService;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/mqtt", post(verify_mqtt))
}

#[derive(serde::Deserialize)]
struct MqttAuth {
    username: String,
    password: String,
    client: String
}

#[derive(serde::Serialize)]
struct MqttAuthResp {
    result: String,
}

impl IntoResponse for MqttAuthResp {
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}

async fn verify_mqtt(
    State(state): State<AppState>,
    SnJson(user): SnJson<MqttAuth>
) -> MqttAuthResp {
    if user.client.starts_with(&user.username) {
        let resp = IntegrationService::query_one(user.password.as_str(), &state.db).await;
        if let Ok(mqtt) = resp {
            if mqtt.enable {
                return MqttAuthResp {
                    result: "allow".to_string(),
                }
            }
        }
    }
    MqttAuthResp {
        result: "deny".to_string(),
    }
}