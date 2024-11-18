use base64::Engine;
use redis::Msg;
use tokio_stream::StreamExt;
use tracing::{info, warn};
use common_define::ClientId;
use common_define::event::{DeviceType, DownEvent};
use device_info::snap::SnapDeviceInfo;
use crate::{DeviceError, DeviceResult};
use crate::man::data::DownloadData;
use crate::man::lora::LoRaNodeManager;
use crate::man::mqtt::SnapPublisher;
use crate::man::redis_client::{RedisClient, RedisRecv};
use crate::protocol::snap::DownJson;

pub struct DownlinkManager {
    recv: RedisRecv
}
impl DownlinkManager {
    pub fn new(recv: RedisRecv) -> DownlinkManager {
        DownlinkManager {
            recv
        }
    }
    
    pub async fn start_downlink(&mut self) {
        let mut s = self.recv.message();
        loop {
            while let Some(msg) = s.next().await {
                match Self::process_downlink(msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Error processing downlink: {}", e);
                    }
                }
            }
        }
    }
    
    async fn process_downlink(msg: Msg) -> DeviceResult {
        let down: DownEvent = serde_json::from_slice(msg.get_payload_bytes())?;
        match down.device {
            DeviceType::LoRaNode => {
                match LoRaNodeManager::get_node_by_eui(down.eui).await? {
                    Some(s) => {
                        let data = base64::engine::general_purpose::STANDARD.decode(down.data.as_bytes())?;
                        info!("{}, down message", down.eui);
                        tokio::spawn(async move {
                            if let Err(e) = s.dispatch_task_now(DownloadData::new_data_with_id_and_forward(data, 1, ClientId::next(), down.port)).await {
                                info!(
                                    device= down.eui.to_string(),
                                    "forward error: {}", e);
                            }
                        });
                    }
                    None => {
                        return Err(DeviceError::Device("not found device".to_string()))
                    }
                }
            }
            DeviceType::Snap => {
                let mut conn = RedisClient::get_client().get_multiplexed_conn().await?;
                let snap = SnapDeviceInfo::load(down.eui, &mut conn).await?
                    .ok_or_else(||  {
                       DeviceError::Device("Error loading snap".to_string()) 
                    })?;
                if let Some(down_link) = snap.down {
                    if snap.freq.is_none() { 
                        return Err(DeviceError::Device("snap device not found freq".to_string()))
                    }
                    match crate::protocol::snap::DownloadData::new_with_eui(down.eui)
                        .set_ack()
                        .set_counter((snap.up_count + 1) as u16)
                        .encode_payload(&[], &snap.key) {
                        Ok(o) => {
                            let j = DownJson {
                                token: rand::random(),
                                freq: snap.freq.unwrap(),
                                data: o,
                            };
                            let s = serde_json::to_string(&j)?;
                            if let Err(e) = SnapPublisher::publish(down_link, s).await {
                                warn!("failed to publish down: {}", e);
                            }

                        }
                        Err(e) => {
                            warn!("failed to encode ack: {:?}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}