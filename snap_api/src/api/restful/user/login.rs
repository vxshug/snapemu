use axum::Extension;
use axum::extract::State;
use crate::error::{ApiResponseResult};
use crate::service::user::{Token, LoginUser, UserService, UserLang};
use crate::api::{SnJson, SnPath};
use crate::AppState;
use crate::man::{UserManager};


/// User login
#[utoipa::path(
    method(post),
    path = "/login",
    security(
        (),
    ),
    request_body = LoginUser,
    responses(
        (status = OK, description = "Success", body = Token)
    ),
    tag = crate::USER_TAG
)]
pub(crate) async fn user_login(
    State(state): State<AppState>,
    lang: UserLang,
    SnJson(user): SnJson<LoginUser>,
) -> ApiResponseResult<Token> {
    let token = UserService::login(
        lang,
        user,
        &state
    ).await?;
    Ok(token.into())
}


pub(crate) async fn verify_email(
    lang: UserLang,
    State(state): State<AppState>,
    Extension(user_manager): Extension<UserManager>,
    SnPath(token): SnPath<String>
) -> ApiResponseResult<String> {
    let user = UserService::verify_active_token(token.as_str(), user_manager, &state.db).await?;
    Ok(user.into())
}
