use std::sync::Mutex;
use std::time::Duration;
use derive_new::new;
use rumqttc::{Event, Incoming, MqttOptions, QoS};
use tokio::sync::mpsc;
use tracing::{debug, error};
use crate::{DeviceError, DeviceResult};
use crate::load::{load_config, MqttConfig};

#[derive(Debug, new)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: bytes::Bytes
}

static LOCAL_CLIENT: Mutex<Option<SnapPublisher>> = Mutex::new(None);

#[derive(Debug, Clone)]
pub struct SnapPublisher {
    client: rumqttc::AsyncClient
}

impl SnapPublisher {
    pub async fn publish(topic: impl Into<String>, payload: impl Into<Vec<u8>> ) -> DeviceResult {
        let client = {
            LOCAL_CLIENT.lock().unwrap().clone()
        };
        if let Some(client) = client {
            client.client.publish(topic, QoS::AtMostOnce, false, payload)
                .await
                .map_err(|e| DeviceError::Connect(e.to_string()))?;
        }
        Ok(())
        
    }
}

pub struct SnapSubscriber {
    mqtt: rumqttc::EventLoop,
    sender: mpsc::Sender<MqttMessage>,
}


impl SnapSubscriber {
    pub async fn new_with_sender(sender: mpsc::Sender<MqttMessage>) -> DeviceResult<Option<Self>> {
        let config = load_config();
        match config.snap {
            None => {
                Ok(None)
            }
            Some(ref snap_config) => {
                let (client, eventloop) = Self::connect(&snap_config.mqtt);
                for topic in &snap_config.mqtt.topic.clone().unwrap_or_default() {
                    client.subscribe(topic, QoS::ExactlyOnce).await.map_err(|e| DeviceError::Connect(e.to_string()))?;
                }
                let _ = LOCAL_CLIENT.lock().unwrap().insert(SnapPublisher { client });
                Ok(Some(Self {
                    sender,
                    mqtt: eventloop,
                }))
            }
        }
    }

    fn connect(config: &MqttConfig) -> (rumqttc::AsyncClient, rumqttc::EventLoop) {
        let mut mqttoptions = MqttOptions::new(config.client.as_str(), config.host.as_str(), config.port);
        mqttoptions.set_credentials(config.username.as_str(), config.password.as_str());
        mqttoptions.set_keep_alive(Duration::from_secs(20));
        rumqttc::AsyncClient::new(mqttoptions, 10)
    }

    pub async fn start(mut self) {
        loop {
            if let Err(e) = self.process_on().await {
                error!("Snap Mqtt error: {}", e);
            }
        }
    }

    async fn process_on(&mut self) -> DeviceResult {
        loop {
            match self.mqtt.poll().await {
                Ok(e) => {
                    if let Event::Incoming(Incoming::Publish(p))  = e {
                        if p.topic.as_str().ends_with("up") {
                            debug!("mqtt message topic: {}", p.topic.as_str());
                            self.sender.send(MqttMessage {
                                topic: p.topic,
                                payload: p.payload
                            }).await.map_err(|e| DeviceError::Connect(e.to_string()))?;
                        }
                    }
                }
                Err(e) => return Err(DeviceError::Connect(e.to_string())),
            }
        }
    }
}
