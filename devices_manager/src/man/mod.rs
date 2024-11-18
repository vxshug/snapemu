
pub(crate) mod lora;
pub(crate) mod data;
mod mq;
mod decode;
mod downlink;
pub mod redis_client;
pub mod mqtt;

pub use downlink::DownlinkManager;

pub use mq::MQ;
pub use decode::DecodeManager;
use common_define::product::ProductType;

pub(crate) type Id = common_define::Id;
#[derive(derive_more::From, Debug, Clone, Copy, serde::Serialize, serde::Deserialize, redis_macros::FromRedisValue, redis_macros::ToRedisArgs)]
#[serde(transparent)]
pub(crate) struct ProductTypeLocal(ProductType);

