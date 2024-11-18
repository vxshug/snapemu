use axum::extract::State;
use axum::Router;
use axum::routing::get;
use common_define::Id;
use common_define::time::Timestamp;
use crate::{get_current_user, AppState, GLOBAL_PRODUCT_NAME};
use crate::api::SnPath;
use crate::error::ApiResponseResult;
use crate::service::device::DeviceService;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(get_all_product))
        .route("/:id", get(get_product))
}


#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
struct ProductInfo {
    pub id: Id,
    pub sku: String,
    pub name: String,
    pub description: String,
    pub image: String,
    pub create_time: Timestamp,
}

async fn get_all_product() -> ApiResponseResult<Vec<ProductInfo>> {
    let products: Vec<_> = GLOBAL_PRODUCT_NAME.get_all_product()
        .into_iter().map(|it| ProductInfo { id: it.id, sku: it.sku, name: it.name, description: it.description, image: it.image, create_time: it.create_time })
        .collect();

    Ok(products.into())
}

async fn get_product(
    State(state): State<AppState>,
    SnPath(id): SnPath<Id>,
) -> ApiResponseResult<Option<ProductInfo>> {
    let user = get_current_user();
    let device_with_auth = DeviceService::query_one_with_auth(user.id, id, &state.db).await?;
    let product = GLOBAL_PRODUCT_NAME.get_by_id(device_with_auth.device.product_id)
        .map(|it| ProductInfo { id: it.id, sku: it.sku, name: it.name, description: it.description, image: it.image, create_time: it.create_time });
    Ok(product.into())
}