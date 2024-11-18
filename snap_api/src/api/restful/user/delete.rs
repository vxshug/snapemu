use axum::extract::State;
use axum::Router;
use axum::routing::post;
use crate::api::SnJson;
use crate::{AppString, tt, AppState};
use crate::error::{ApiError, ApiResponseResult};
use crate::man::user_status::UserStatus;
use crate::service::user::{UserDelete, UserLang, UserReset, UserService};
use crate::utils::Checker;



pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/request", post(request_delete))
        .route("/", post(delete_user))
}


pub(crate) async fn request_delete(
    lang: UserLang,
    State(state): State<AppState>,
    status: UserStatus,
    SnJson(info): SnJson<UserReset>
) -> ApiResponseResult<AppString> {
    if !Checker::email(info.email.as_str()) {
        return Err(
            ApiError::User(
                tt!("messages.user.signup.email_format", email = info.email)
            )
        )
    }

    let s = UserService::delete(status, &info, &state).await?;
    Ok(s.into())
}

pub(crate) async fn delete_user(
    lang: UserLang,
    State(state): State<AppState>,
    status: UserStatus,
    SnJson(info): SnJson<UserDelete>
) -> ApiResponseResult<String> {
    if !Checker::email(info.email.as_str()) {
        return Err(
            ApiError::User(
                tt!("messages.user.signup.email_format", email = info.email)
            )
        )
    }
    if info.password.len() < 8 {
        return Err(
            ApiError::User(
                tt!("messages.user.signup.password_format")
            )
        )
    }
    
    
    let s = UserService::delete_user(status, &info, &state).await?;
    Ok(s.into())
}

