use serde::{Deserialize, Serialize};
use crate::db::{Eui, LoRaAddr};
use crate::Id;

#[derive(Serialize, Deserialize, Clone)]
pub struct JoinRequest {
    pub app_eui: Eui,
    pub dev_eui: Eui,
    pub time: i64
}

#[derive(Serialize, Deserialize, Clone)]
pub struct JoinAccept {
    pub dev_addr: LoRaAddr,
    pub time: i64
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UplinkData {
    pub dev_addr: LoRaAddr,
    pub confirm: bool,
    pub f_port: i32,
    pub f_cnt: i32,
    pub payload: Option<String>,
    pub decoded_payload: Option<String>,
    pub gateway: GatewayRxStatus,
    pub time: i64
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GatewayRxStatus {
    pub id: Id,
    pub eui: Eui,
    pub time: i64,
    pub rssi: i32,
    pub snr: f32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DownLinkData {
    pub confirm: bool,
    pub f_port: i32,
    pub bytes: Option<String>,
    pub time: i64
}

#[cfg(test)]
mod tests {
}