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
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};
use common_define::db::{DbErr, DeviceLoraGateColumn, DeviceLoraGateEntity, Eui};
use common_define::event::DownloadMessage;
use crate::event::{config_cache, update_config_cache};
use crate::man::gw::{DataWrapper, GwCmd, GwCmdResponse, ShellCmd};
use crate::man::Id;
use crate::man::lora::{LoRaNode, LoRaNodeManager};
use crate::protocol::mqtt::{LinkRx, LinkTx, Notification};


#[derive(Debug, Deserialize)]
pub struct MqttDownloadMessage {
    #[serde(default)]
    pub port: u8,
    pub payload: String,
}

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
    down: mpsc::Sender<DownloadMessage>
}

impl MessageProcessor {
    pub fn new_with_sender(rx: LinkRx, sender: mpsc::Sender<MqttMessage>, down: mpsc::Sender<DownloadMessage>) -> Self {
        Self { sender, rx, down }
    }


    pub async fn start(mut self) {
        while let Ok(message) = self.rx.next().await {
            if let Some(Notification::Forward(publish)) = message {
                match String::from_utf8(publish.publish.topic.to_vec()) {
                    Ok(topic) => {
                        tokio::spawn(process_mqtt(MqttMessage::new(topic, publish.publish.payload), self.down.clone()));
                    }
                    Err(_) => {
                        warn!("Snap Mqtt message contained invalid UTF-8");
                    }
                }
            }
        }
    }
}

async fn process_mqtt(message: MqttMessage, down: mpsc::Sender<DownloadMessage>) -> Result<(), MqttError> {
    let mut topic = message.topic.splitn(3, '/');
    topic.next();
    if let Some(user_id) = topic.next() {
        let id = Id::from_str(user_id)?;
        let topic = topic.next().ok_or(MqttError::TopicNotFound)?;
        if topic.starts_with("gw") {
            process_gateway(id, message).await?;
            return Ok(());
        }
        if topic.starts_with("device") {
            process_gateway(id, message).await?;
            return Ok(());
        }
    }
    Ok(())
}
async fn process_downlink(user_id: Id, message: MqttMessage, down: mpsc::Sender<DownloadMessage>) -> Result<(), MqttError> {
    let mut topic = message.topic.split( '/');
    topic.next();
    topic.next();
    topic.next();
    let eui_s = topic.next().ok_or(MqttError::EuiNotFound)?;
    let action = topic.next().ok_or(MqttError::ActionNotFound)?;
    let eui = Eui::from_str(eui_s)?;
    if action == "forward" {
        if let Some(node) = LoRaNodeManager::get_node_by_eui(eui).await? {
            if node.info.user_id == Some(user_id) {
                let data: MqttDownloadMessage = serde_json::from_slice(&message.payload)?;
                down.send(DownloadMessage {
                    eui,
                    port: data.port,
                    data: data.payload,
                }).await;
            }
        }
    }
    Ok(())
}
async fn process_gateway(user_id: Id, message: MqttMessage) -> Result<(), MqttError> {
    let mut topic = message.topic.split( '/');
    topic.next();
    topic.next();
    topic.next();
    let eui_s = topic.next().ok_or(MqttError::EuiNotFound)?;
    let action = topic.next().ok_or(MqttError::ActionNotFound)?;
    if "up" == action {
        let cmd: GwCmdResponse = serde_json::from_slice(message.payload.as_ref()).map_err(|e| MqttError::SerdeError(e))?;
        let eui = Eui::from_str(eui_s)?;
        let gate = DeviceLoraGateEntity::find().filter(DeviceLoraGateColumn::Eui.eq(eui)).one(&GLOBAL_STATE.db).await?;
        if let Some(gate) = gate {
            match cmd {
                GwCmdResponse::ShellCmd(DataWrapper { data }) => {}
                GwCmdResponse::Config(DataWrapper { data: config }) => {
                    if let Some(sender) = config_cache(&gate.device_id) {
                        sender.send(config);
                    }
                }
                GwCmdResponse::UpdateConfig(DataWrapper { data: config }) => {
                    if let Some(sender) = update_config_cache(&gate.device_id) {
                        sender.send(config);
                    }
                }
            }
        }
    }
    Ok(())
}
