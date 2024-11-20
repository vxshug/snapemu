use std::sync::Arc;
use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use serde::Deserialize;
use tracing::info;
use snap_config::{DeviceTopicConfig, SnapConfig};

use crate::Topic;
use crate::protocol::lora::source::{listen_udp, LoRaUdp};


static CONFIG: Lazy<ArcSwap<DeviceConfig>> = Lazy::new(|| { ArcSwap::new(Arc::new(DeviceConfig::default())) });


pub fn store_config(config: String, env_prefix: String) -> arc_swap::Guard<Arc<DeviceConfig>> {
    if !std::path::Path::new(&config).exists() {
        eprintln!("not fount config file in {}", config);
        let config = SnapConfig::builder()
            .env_prefix(&env_prefix)
            .build().unwrap();
        CONFIG.store(Arc::new(config.into_local_config().unwrap()));
        return load_config()
    }
    let config = SnapConfig::builder()
        .add_file(&config)
        .env_prefix(&env_prefix)
        .build().unwrap();
    CONFIG.store(Arc::new(config.into_local_config().unwrap()));
    load_config()

}
pub fn load_config() -> arc_swap::Guard<Arc<DeviceConfig>> {
    CONFIG.load()
}

#[derive(Deserialize, Debug, Default)]
pub struct DeviceConfig {
    #[serde(default)]
    pub db: snap_config::DatabaseConfig,
    #[serde(default)]
    pub redis: snap_config::RedisConfig,
    #[serde(default)]
    pub log: snap_config::LogLevelConfig,
    #[serde(default)]
    pub device: DeviceConfigInner,
    #[serde(default)]
    pub mqtt: Option<MqttConfig>,
    #[serde(default)]
    pub snap: Option<SnapDeviceConfig>
}

#[derive(Deserialize, Debug, Default)]
pub struct DeviceConfigInner {
    pub topic: DeviceTopicConfig,
    pub lorawan: LoRaConfig
}

#[derive(Deserialize, Debug)]
pub struct LoRaConfig {
    #[serde(default="_default_lora_host")]
    pub host: String,
    #[serde(default="_default_lora_port")]
    pub port: u16,
}

impl Default for LoRaConfig {
    fn default() -> Self {
        Self {
            host: _default_lora_host(),
            port: _default_lora_port(),
        }
    }
}

fn _default_lora_host() -> String {
    "localhost".to_string()
}
fn _default_lora_port() -> u16 {
    1700
}

#[derive(Deserialize, Debug)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub client: String,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub tls: bool,
    #[serde(default)]
    pub topic: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct SnapDeviceConfig {
    pub mqtt: MqttConfig
}


pub struct State {
    pub db: sea_orm::DatabaseConnection,
    pub udp: LoRaUdp,
}
pub(crate) fn load_state() -> State {
    tokio::task::block_in_place(move || {
        tokio::runtime::Handle::current().block_on(async move {
            let db = load_db().await;
            let (forward, udp) = listen_udp().await.unwrap();
            tokio::spawn(async move {
                forward.start().await;
            });
            State {
                db,
                udp,
            }
        })
    })
}


async fn load_db() -> sea_orm::DatabaseConnection  {
    let config = load_config();
    let username = config.db.username.clone();
    let password = config.db.password.clone();
    let port = config.db.port;
    let count = config.db.connection_count;
    let db = config.db.db.clone();
    let host = config.db.host.clone();
    info!(
                event = "config",
                "type" = "db",
                host  = host,
                "DB Config success"
            );
    let url = format!("postgres://{username}:{password}@{host}:{port}/{db}");
    let mut option = sea_orm::ConnectOptions::new(url);
    option.max_connections(count as _);
    option.sqlx_logging(config.db.sqlx_logging);
    sea_orm::Database::connect(option).await.unwrap()
}



pub(crate) fn load_topic() -> Topic {
    let config = load_config();
    let data = config.device.topic.data.clone();
    let event = config.device.topic.event.clone();
    let down = config.device.topic.down.clone();

    
    Topic {
        data: Box::leak(data.into_boxed_str()),
        gate_event: Box::leak(event.into_boxed_str()),
        down: Box::leak(down.into_boxed_str()),
    }
}


