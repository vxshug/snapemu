use std::str::FromStr;
use std::string::FromUtf8Error;
use crate::load::{load_config, MqttConfig};
use crate::{DeviceError, DeviceResult, GLOBAL_STATE};
use derive_new::new;
use rumqttc::{Event, Incoming, MqttOptions, QoS};
use std::sync::Mutex;
use std::time::Duration;
use bytes::Bytes;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};
use common_define::db::{DbErr, DeviceLoraGateColumn, DeviceLoraGateEntity, Eui};
use crate::man::gw::{GwCmd, GwCmdResponse, GwMsgType, MsgData};
use crate::man::Id;
use crate::protocol::mqtt::{LinkRx, LinkTx, Notification};


const MAX_SEND_SIZE: usize = 900;

#[derive(Debug, new)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: bytes::Bytes,
}

pub struct MqPublisher {
    client: Mutex<LinkTx>,
}

#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("UserNotFound")]
    UserNotFound,
    #[error("ProductNotFound")]
    ProductNotFound,
    #[error("EuiNotFound")]
    EuiNotFound,
    #[error("TopicNotFound")]
    TopicNotFound,
    #[error("ActionNotFound")]
    ActionNotFound,
    #[error("DbErr {0}")]
    DbErr(#[from] DbErr),
    #[error("serde_json {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("SeaDb {0}")]
    SeaDb(#[from] sea_orm::DbErr),
    #[error("DeviceError {0}")]
    DeviceError(#[from] DeviceError),
}

impl MqPublisher {

    pub fn new(client: LinkTx) -> MqPublisher {
        MqPublisher { client: Mutex::new(client) }
    }
    pub async fn publish(&self, message: crate::integration::mqtt::MqttMessage) -> DeviceResult {
        let mut client = self.client.lock().unwrap();
        client.publish(message.topic, message.message).unwrap();
        Ok(())
    }
}

pub struct MessageProcessor {
    rx: LinkRx,
    sender: mpsc::Sender<MqttMessage>,
}

impl MessageProcessor {
    pub fn new_with_sender(rx: LinkRx, sender: mpsc::Sender<MqttMessage>) -> Self {
        Self { sender, rx }
    }


    pub async fn start(mut self) {
        while let Ok(message) = self.rx.next().await {
            if let Some(Notification::Forward(publish)) = message {
                match String::from_utf8(publish.publish.topic.to_vec()) {
                    Ok(topic) => {
                        tokio::spawn(process_mqtt(MqttMessage::new(topic, publish.publish.payload)));
                    }
                    Err(_) => {
                        warn!("Snap Mqtt message contained invalid UTF-8");
                    }
                }
            }
        }
    }
}

async fn process_mqtt(message: MqttMessage) -> Result<(), MqttError> {
    let mut topic = message.topic.splitn(3, '/');
    topic.next();
    if let Some(user_id) = topic.next() {
        let id = Id::from_str(user_id)?;
        let topic = topic.next().ok_or(MqttError::TopicNotFound)?;
        if topic.starts_with("gw") {
            process_gateway(id, message).await?;
        }
    }
    Ok(())
}

async fn process_gateway(user_id: Id, message: MqttMessage) -> Result<(), MqttError> {
    let mut topic = message.topic.split( '/');
    topic.next();
    topic.next();
    topic.next();
    let product = topic.next().ok_or(MqttError::ProductNotFound)?;
    let eui_s = topic.next().ok_or(MqttError::EuiNotFound)?;
    let action = topic.next().ok_or(MqttError::ActionNotFound)?;
    if "up" == action {
        let response: serde_json::Result<GwCmdResponse> =  serde_json::from_slice(message.payload.as_ref());
        if response.is_ok() {
            return Ok(())
        }
        let eui = Eui::from_str(eui_s)?;
        let gate = DeviceLoraGateEntity::find().filter(DeviceLoraGateColumn::Eui.eq(eui)).one(&GLOBAL_STATE.db).await?;
        if let Some(gate) = gate {
            if !gate.config.is_empty() {
                let config = gate.config;
                let mut send_length = 0usize;
                let down_topic = format!("user/{}/gw/{}/{}/down", user_id, product, eui_s);

                let mut payload = GwCmd::new(product.to_string(), GwMsgType::ShellCmd, MsgData::new("echo -n '' > /root/lora/packet_forwarder/lora_pkt_fwd/global_conf.json".to_string(), 1000));
                let p = serde_json::to_string(&payload)?;
                GLOBAL_STATE.mq.publish(crate::integration::mqtt::MqttMessage::new(p, down_topic.clone())).await?;
                loop {
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    if (config.len() - send_length) < MAX_SEND_SIZE {
                        payload.data.cmd_str = format!("echo -n '{}' >> /root/lora/packet_forwarder/lora_pkt_fwd/global_conf.json", &config[send_length..]);
                        send_length += config.len();
                    } else {
                        payload.data.cmd_str = format!("echo -n '{}' >> /root/lora/packet_forwarder/lora_pkt_fwd/global_conf.json", &config[send_length..send_length+MAX_SEND_SIZE]);
                        send_length += MAX_SEND_SIZE;
                    };
                    let p = serde_json::to_string(&payload)?;
                    GLOBAL_STATE.mq.publish(crate::integration::mqtt::MqttMessage::new(p, down_topic.clone())).await?;
                    if send_length >= config.len() {
                        break;
                    }
                }
            }
        }

    }
    Ok(())
}
