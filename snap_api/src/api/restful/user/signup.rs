use axum::extract::State;
use crate::error::{ApiError, ApiResponseResult};
use crate::service::user::{UserInfo, UserLang, UserReset, UserResetPassword, UserService};
use crate::api::{SnJson};
use crate::man::user_status::UserStatus;
use crate::{AppString, tt, AppState};
use crate::utils::Checker;


pub(crate) async fn user_signup(
    State(state): State<AppState>,
    SnJson(info): SnJson<UserInfo>
) -> ApiResponseResult<AppString> {

    if info.password.len() < 8 {
        return Err(
            ApiError::User(
                tt!("messages.user.signup.password_format")
            )
        )
    }
    UserService::signup(info, &state).await?;
    Ok(tt!("messages.user.signup.success").into())
}


pub(crate) async fn reset_password(
    State(state): State<AppState>,
    status: UserStatus,
    SnJson(info): SnJson<UserResetPassword>
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
    let s = UserService::reset_password(status, &info, &state).await?;
    Ok(s.into())
}


pub(crate) async fn reset(
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

    let s = UserService::reset(status, &info, &state).await?;
    Ok(s.into())
}
