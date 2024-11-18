use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use crate::event::lora_gateway::GatewayEvent;
use crate::event::lora_node::{DownLinkData, JoinAccept, JoinRequest, UplinkData};
use crate::Id;


pub mod lora_node;
pub mod lora_gateway;
mod log;
pub use log::PlatformLog;
use crate::db::Eui;

#[derive(Serialize, Deserialize, Clone)]
pub struct DeviceEvent {
    pub device: Id,
    pub event: DeviceEventType
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "event")]
pub enum DeviceEventType {
    JoinRequest(JoinRequest),
    JoinAccept(JoinAccept),
    UplinkData(UplinkData),
    DownLinkData(DownLinkData),
    Gateway(GatewayEvent),
    SnapDevice(SnapEvent)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SnapEvent {
    pub eui: Eui,
    pub data: Vec<u8>
}

#[derive(Serialize_repr, Deserialize_repr, Clone)]
#[repr(u8)]
pub enum DeviceType {
    LoRaNode = 1,
    Snap = 2
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DownEvent {
    pub device: DeviceType,
    pub eui: Eui,
    pub port: u8,
    pub data: String,
}

impl DeviceEvent {
    pub const KAFKA_TOPIC: &'static str = "LoRaNode-Event";
    pub const DOWN_TOPIC: &'static str = "Device-Downlink";
}