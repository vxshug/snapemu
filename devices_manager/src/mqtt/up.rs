use crate::load::{load_config, MqttConfig};
use crate::man::mqtt::MqttMessage;
use crate::{DeviceError, DeviceResult};
use rumqttc::{Event, Incoming, MqttOptions, QoS};
use std::sync::{mpsc, Mutex};
use std::time::Duration;
use tracing::error;

static LOCAL_CLIENT: Mutex<Option<MqttPublisher>> = Mutex::new(None);

#[derive(Debug, Clone)]
pub struct MqttPublisher {
    client: rumqttc::AsyncClient,
}

impl MqttPublisher {
    pub async fn publish(topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> DeviceResult {
        let client = { LOCAL_CLIENT.lock().unwrap().clone() };
        if let Some(client) = client {
            client
                .client
                .publish(topic, QoS::AtMostOnce, false, payload)
                .await
                .map_err(|e| DeviceError::Connect(e.to_string()))?;
        }
        Ok(())
    }
}

pub struct MqttSubscriber {
    mqtt: rumqttc::EventLoop,
    sender: mpsc::Sender<MqttMessage>,
}

impl MqttSubscriber {
    pub async fn new_with_sender(sender: mpsc::Sender<MqttMessage>) -> DeviceResult<Option<Self>> {
        let config = load_config();
        let mqtt_config = MqttConfig {
            host: "".to_string(),
            port: 0,
            username: "".to_string(),
            password: "".to_string(),
            client: "".to_string(),
            ca: None,
            tls: false,
            topic: None,
        };
        match config.mqtt {
            None => Ok(None),
            Some(ref config) => {
                let (client, eventloop) = Self::connect(&mqtt_config);
                for topic in &mqtt_config.topic.clone().unwrap_or_default() {
                    client
                        .subscribe(topic, QoS::ExactlyOnce)
                        .await
                        .map_err(|e| DeviceError::Connect(e.to_string()))?;
                }
                let _ = LOCAL_CLIENT.lock().unwrap().insert(MqttPublisher { client });
                Ok(Some(Self { sender, mqtt: eventloop }))
            }
        }
    }

    fn connect(config: &MqttConfig) -> (rumqttc::AsyncClient, rumqttc::EventLoop) {
        let mut mqttoptions =
            MqttOptions::new(config.client.as_str(), config.host.as_str(), config.port);
        mqttoptions.set_credentials(config.username.as_str(), config.password.as_str());
        mqttoptions.set_keep_alive(Duration::from_secs(20));
        rumqttc::AsyncClient::new(mqttoptions, 10)
    }

    pub async fn start(mut self) {
        loop {
            if let Err(e) = self.process_on().await {
                error!("Mqtt error: {}", e);
            }
        }
    }

    async fn process_on(&mut self) -> DeviceResult {
        loop {
            match self.mqtt.poll().await {
                Ok(e) => {
                    if let Event::Incoming(Incoming::Publish(p)) = e {
                        self.sender
                            .send(MqttMessage { topic: p.topic, payload: p.payload })
                            .map_err(|e| DeviceError::Connect(e.to_string()))?;
                    }
                }
                Err(e) => return Err(DeviceError::Connect(e.to_string())),
            }
        }
    }
}
