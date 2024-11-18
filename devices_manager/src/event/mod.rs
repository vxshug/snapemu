pub mod gateway;

mod platform;

use base64::Engine;
use lorawan::parser::{AsPhyPayloadBytes, DataHeader};
use redis::AsyncCommands;
use common_define::db::LoRaAddr;
use common_define::event::lora_node::GatewayRxStatus;
use device_info::lorawan::NodeInfo;
use utils::base64::EncodeBase64;
use crate::DeviceResult;
use crate::man::data::DownloadData;
use crate::man::lora::{LoRaNode};
use crate::protocol::lora::payload::LoRaPayload;
use crate::service::lorawan_node::PushData;

pub struct LoRaNodeEvent;


impl LoRaNodeEvent {

    pub(crate) async fn join_request(
        _data: &PushData,
        device: &NodeInfo,
        conn: &mut redis::aio::MultiplexedConnection
    ) -> DeviceResult {
        let resp = common_define::event::DeviceEvent {
            device: device.device_id,
            event: common_define::event::DeviceEventType::JoinRequest(
            common_define::event::lora_node::JoinRequest {
                app_eui: device.app_eui,
                dev_eui: device.dev_eui,
                time: chrono::Utc::now().timestamp_millis(),
            }
        )};
        let resp = serde_json::to_string(&resp)?;
        conn.publish(
            common_define::event::DeviceEvent::KAFKA_TOPIC,
             resp
        ).await?;
        Ok(())
    }

    pub(crate) async fn join_accept(
        addr: LoRaAddr,
        device: &NodeInfo,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> DeviceResult {
        let resp = common_define::event::DeviceEvent {
            device: device.device_id,
            event: common_define::event::DeviceEventType::JoinAccept(
            common_define::event::lora_node::JoinAccept {
                dev_addr: addr,
                time: chrono::Utc::now().timestamp_millis(),
            }
        )};
        let resp = serde_json::to_string(&resp)?;
        conn.publish(
            common_define::event::DeviceEvent::KAFKA_TOPIC,
            resp
        ).await?;
        Ok(())
    }

    pub(crate) async fn uplink(
        header: &LoRaPayload,
        device: &LoRaNode,
        gateway: &PushData,
        data: &[u8],
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> DeviceResult {
        let gateway = GatewayRxStatus {
            id: gateway.gateway,
            eui: gateway.eui,
            time: gateway.time.timestamp_millis() as i64,
            rssi: gateway.pk.rssi,
            snr: gateway.pk.lsnr,
        };
        let resp = common_define::event::DeviceEvent {
            device: device.info.device_id,
            event: common_define::event::DeviceEventType::UplinkData(
            common_define::event::lora_node::UplinkData {
                dev_addr: device.info.dev_addr,
                confirm: header.is_confirmed(),
                f_port: header.f_port().unwrap_or_default() as i32,
                f_cnt: header.fhdr().fcnt() as _ ,
                payload: Some(header.as_bytes().encode_base64()),
                decoded_payload: Some(data.encode_base64()),
                gateway,
                time: chrono::Utc::now().timestamp_millis(),
            }
        )};
        let resp = serde_json::to_string(&resp)?;
        conn.publish(
            common_define::event::DeviceEvent::KAFKA_TOPIC,
            resp
        ).await?;
        Ok(())
    }

    pub(crate) async fn down_link(
        _data: &PushData,
        device: &NodeInfo,
        down: Option<&DownloadData>,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> DeviceResult {
        let resp = common_define::event::DeviceEvent {
            device: device.device_id,
            event: common_define::event::DeviceEventType::DownLinkData(
            common_define::event::lora_node::DownLinkData {
                confirm: false,
                f_port: down.map(|i| i.port).unwrap_or(2) as i32,
                bytes: down.map(|i| base64::engine::general_purpose::STANDARD.encode(i.bytes.as_ref())),
                time: chrono::Utc::now().timestamp_millis(),
            }
        )};
        let resp = serde_json::to_string(&resp)?;
        conn.publish(
            common_define::event::DeviceEvent::KAFKA_TOPIC,
            resp
        ).await?;
        Ok(())
    }
}
