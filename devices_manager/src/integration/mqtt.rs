use serde_json::Value;
use common_define::db::{Eui, LoRaAddr};
use crate::DeviceResult;
use crate::man::data::ValueType;
use crate::man::Id;

pub(crate) struct MqttMessage {
    message: String,
    topic: String,
    qos: i32,
}

// impl IntoMQTTMessage for MqttMessage {
//     fn topic(&self) -> &str {
//         self.topic.as_str()
//     }
// 
//     fn payload(&self) -> &[u8] {
//         self.message.as_bytes()
//     }
// 
//     fn qos(&self) -> i32 {
//         self.qos
//     }
// }


impl MqttMessage {
    pub(crate) fn new_one_data(data: &MqttData, qos: i32) -> DeviceResult<Self> {
        let message = serde_json::to_string(data)?;
        let topic = format!("/v1/device/{}/data/{}", data.device.unwrap(), data.data_id);
        Ok(Self {
            message,
            topic,
            qos,
        })
    }

    pub(crate) fn new_data(data: &MqttDataAll, qos: i32) -> DeviceResult<Self> {
        let message = serde_json::to_string(data)?;
        let topic = format!("/v1/device/{}/data", data.device);
        Ok(Self {
            message,
            topic,
            qos,
        })
    }

    pub(crate) fn new_row_data(data: &MqttRawData, qos: i32) -> DeviceResult<Self> {
        let message = serde_json::to_string(data)?;
        let topic = format!("/v1/device/{}/row", data.device);
        Ok(Self {
            message,
            topic,
            qos,
        })
    }

    pub(crate) fn new_decode_data(data: &MqttDecodeData, qos: i32) -> DeviceResult<Self> {
        let message = serde_json::to_string(data)?;
        let topic = format!("/v1/device/{}/decode", data.device);
        Ok(Self {
            message,
            topic,
            qos,
        })
    }

    pub(crate) fn new_decode_group_data(data: &MqttDecodeData, group_id: Id, qos: i32) -> DeviceResult<Self> {
        let message = serde_json::to_string(data)?;
        let topic = format!("/v1/group/{}/decode", group_id);
        Ok(Self {
            message,
            topic,
            qos,
        })
    }
}

#[derive(serde::Serialize)]
pub(crate) struct MqttData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) device: Option<Id>,
    pub(crate) data_id: i32,
    pub(crate) s_id: i32,
    pub(crate) pk_id: i16,
    pub(crate) v_type: ValueType,
    pub(crate) data: Value,
    pub(crate) bytes: String,
}

#[derive(serde::Serialize)]
pub(crate) struct MqttDataAll {
    pub(crate) device: Id,
    pub(crate) data: Vec<MqttData>
}

#[derive(serde::Serialize)]
pub(crate) struct MqttRawData {
    pub(crate) device: Id,
    pub(crate) bytes: String
}

#[derive(serde::Serialize)]
pub(crate) struct MqttDecodeData {
    pub(crate) device: Id,
    pub(crate) bytes: String,
    pub(crate) battery: Option<u8>,
    pub(crate) charge: Option<bool>,
    pub(crate) eui: Eui,
    pub(crate) addr: LoRaAddr,
    pub(crate) data: Vec<MqttDataItem>
}

#[derive(serde::Serialize)]
pub(crate) struct MqttDataItem {
    pub(crate) data: serde_json::Value,
    pub(crate) data_id: i32,
    pub(crate) v_type: ValueType,
    pub(crate) v_name: Option<String>,
    pub(crate) v_unit: Option<String>,
}