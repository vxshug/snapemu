use crate::man::lora::{LoRaGate, LoRaNode, LoRaNodeManager};
use crate::man::Id;
use crate::protocol::lora;
use crate::protocol::lora::payload::LoRaPayload;
use crate::{decode, DeviceError, DeviceResult, GLOBAL_DEPEND, GLOBAL_STATE};
use common_define::db::{DbDecodeData, DeviceDataActiveModel, DeviceLoraNodeColumn, DeviceLoraNodeEntity, DevicesEntity, Eui, Key, LoRaAddr};
use common_define::lora::LoRaJoinType;
use common_define::lorawan_bridge::{GatewayToken, RXPK};
use common_define::time::Timestamp;
use common_define::last_device_data_key;
use lorawan::parser::{DataHeader, DecryptedDataPayload};
use once_cell::sync::Lazy;
use tracing::instrument;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use redis::AsyncCommands;
use sea_orm::{ActiveModelTrait, ActiveValue, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter};
use tracing::{debug, error, info, warn};
use common_define::decode::LastDecodeData;
use device_info::lorawan::NodeInfo;
use utils::base64::EncodeBase64;

use crate::decode::RawData;
use crate::event::LoRaNodeEvent;
use crate::integration::mqtt::{MqttMessage, MqttRawData};
use crate::man::redis_client::RedisClient;
use crate::protocol::lora::join_request::RequestJoin;

struct DataItem {
    push: PushData,
    payload: LoRaPayload,
    data: DecryptedDataPayload<Vec<u8>>,
}

struct RequestCache {
    map: Arc<Mutex<HashMap<String, (PushData, RequestJoin)>>>,
}

impl RequestCache {
    pub(crate) fn insert(
        &self,
        push: PushData,
        req: RequestJoin,
    ) -> bool {
        let mut map = self.map.lock().unwrap();
        let m = Arc::clone(&self.map);
        let key = format!("{}:{}", req.app_eui(), req.dev_eui());
        let remove_key = key.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            m.lock().unwrap().remove(&remove_key);
        });
        match map.get_mut(&key) {
            None => {
                map.insert(
                    key,
                    (push, req),
                );
                true
            }
            Some(queue) => {
                if push.pk.rssi > queue.0.pk.rssi {
                    *queue = (push, req);
                }
                false
            }
        }
    }

    pub(crate) fn get(&self, app_eui: Eui, dev_eui: Eui) -> Option<(PushData, RequestJoin)> {
        let key = format!("{}:{}", app_eui, dev_eui);
        self.map.lock().unwrap().remove(&key)
    }
}

static REQUEST_QUEUE: Lazy<RequestCache> = Lazy::new(|| RequestCache {
    map: Default::default(),
});

struct DataCache {
    map: Arc<Mutex<HashMap<LoRaAddr, DataItem>>>,
}

impl DataCache {
    pub(crate) fn insert(
        &self,
        addr: LoRaAddr,
        push: PushData,
        payload: LoRaPayload,
        data: DecryptedDataPayload<Vec<u8>>,
    ) -> bool {
        let mut map = self.map.lock().unwrap();
        let m = Arc::clone(&self.map);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            m.lock().unwrap().remove(&addr)
        });
        match map.get_mut(&addr) {
            None => {
                map.insert(
                    addr,
                    DataItem {
                        push,
                        payload,
                        data,
                    },
                );
                true
            }
            Some(queue) => {
                let queue_count = queue.payload.fhdr().fcnt();
                let payload_count = payload.fhdr().fcnt();
                if queue_count < payload_count || (queue_count == payload_count && push.pk.rssi > queue.push.pk.rssi) {
                    *queue = DataItem {
                        push,
                        payload,
                        data,
                    };
                }
                false
            }
        }
    }

    pub(crate) fn get(&self, addr: LoRaAddr) -> Option<DataItem> {
        self.map.lock().unwrap().remove(&addr)
    }
}

static DATA_QUEUE: Lazy<DataCache> = Lazy::new(|| DataCache {
    map: Default::default(),
});

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub(crate) struct PushData {
    pub(crate) gateway: Id,
    pub(crate) eui: Eui,
    pub(crate) token: GatewayToken,
    pub(crate) version: u8,
    pub(crate) time: Timestamp,
    pub(crate) pk: RXPK,
}

#[instrument(skip(gw, data))]
pub(crate) async fn node_data(gw: LoRaGate, rssi: i32, data: PushData) {
    if let Err(e) = node_data_decode(gw, data).await {
        warn!("{}", e);
    }
}

pub(crate) async fn node_data_decode(mut gw: LoRaGate, data: PushData) -> DeviceResult {
    
    let phy = lora::parse::LoraMacDecode::switch(data.pk.data.as_bytes())?;

    gw.update_tmst(data.pk.tmst).await?;

    match phy {
        lora::parse::LoraPhy::Request(req) => {
            let app_eui = req.app_eui();
            let dev_eui = req.dev_eui();
            if REQUEST_QUEUE.insert(data, req) {
                tokio::time::sleep(Duration::from_millis(200)).await;
                match REQUEST_QUEUE.get(app_eui, dev_eui) {
                    None => {
                        info!("Discard duplicate requests");
                    }
                    Some((push, req)) => {
                        request_join(
                            app_eui,
                            dev_eui,
                            &push,
                            &req,
                            gw,
                        )
                            .await;
                    }
                }
            }
        }
        lora::parse::LoraPhy::Payload(payload) => {
            let dev_addr = payload.dev_addr();
            let count = payload.fhdr().fcnt();
            decode_enc_payload(payload, dev_addr, count, data, gw).await?;
        }
    }
    Ok(())
}

async fn decode_payload(
    push_data: &PushData,
    node: &mut LoRaNode,
    header: &LoRaPayload,
    payload: DecryptedDataPayload<Vec<u8>>,
) -> DeviceResult {
    node.update_time().await?;
    let conn = &GLOBAL_STATE.db;
    let mut redis = RedisClient::get_client().get_multiplexed_conn().await?;
    let all_data = MqttRawData {
        device: node.info.device_id,
        bytes: push_data.pk.data.clone(),
    };
    let msg = MqttMessage::new_row_data(&all_data, 1)?;
    node.update_gateway().await?;

    for cmd in payload.fhdr().fopts() {
        warn!("fopt command: {:?}", cmd);
    }

    let payload = payload.frm_payload().map_err(DeviceError::data)?;
    match payload {
        lorawan::parser::FRMPayload::Data(data) => {
            tracing::info!("UpLink: {:02X?}", data);
            node.pull_task(data, push_data, header).await?;
            LoRaNodeEvent::uplink(header, node, push_data, data, &mut redis).await?;
            match node.info.script { 
                Some(o) => {
                    let script = common_define::db::DecodeScriptEntity::find_by_id(o)
                        .one(conn)
                        .await?;
                    match script {
                        None => {
                            warn!("Not found Script");
                        }
                        Some(script) => {
                            let bytes_b64 = data.encode_base64();
                            let decodedata = GLOBAL_DEPEND.decode_with_code(o, script.script.as_str(), RawData::new(data)).map_err(|e| DeviceError::data("js decode"))?;
                            if decodedata.data.is_empty() {
                                warn!("js return null");
                                return Ok(())
                            }
                            let last_key = last_device_data_key(node.info.device_id);
                            let data: DbDecodeData = decodedata.into();
                            let now = Timestamp::now();
                            let last_data = LastDecodeData::new(data.0.clone(), now);
                            redis.set(last_key, last_data).await?;
                            let data = DeviceDataActiveModel {
                                id: Default::default(),
                                device_id: ActiveValue::Set(node.info.device_id),
                                data: ActiveValue::Set(data),
                                bytes: ActiveValue::Set(bytes_b64),
                                create_time: ActiveValue::Set(now),
                            };
                            data.insert(conn).await?;
                        }
                    }
                }
                None => {
                    let decoded_data = decode::up_data_decode(data)?;

                    info!("decode {:?}", decoded_data);
                    let bytes_b64 = data.encode_base64();
                    let last_key = last_device_data_key(node.info.device_id);
                    let now = Timestamp::now();
                    let last_data = LastDecodeData::new(decoded_data.data.clone(), now);
                    debug!("save last data");
                    redis.set(last_key, last_data).await?;
                    let data = DeviceDataActiveModel {
                        id: Default::default(),
                        device_id: ActiveValue::Set(node.info.device_id),
                        data: ActiveValue::Set(DbDecodeData(decoded_data.data)),
                        bytes: ActiveValue::Set(bytes_b64),
                        create_time: ActiveValue::Set(Timestamp::now()),
                    };
                    if let Some(status) = decoded_data.status {
                        debug!("change battery status");
                        redis.hset(node.key.as_str(), (NodeInfo::battery(),status.battery), (NodeInfo::charge(), status.charge)).await?;
                    }
                    data.insert(conn).await?;
                }
            }

            return Ok(());
        }
        lorawan::parser::FRMPayload::MACCommands(commands) => {
            for command in commands.mac_commands() {
                warn!("command: {:?}", command);
            }
        }
        lorawan::parser::FRMPayload::None => {}
    }
    Ok(())
}

#[instrument(skip(data, req, gw))]
async fn request_join(
    app_eui: Eui,
    dev_eui: Eui,
    data: &PushData,
    req: &RequestJoin,
    gw: LoRaGate,
) {
    if let Err(e) = request_warp(app_eui, dev_eui, data, req, gw).await {
        warn!("{e}")
    }
}

async fn request_warp(
    app_eui: Eui,
    dev_eui: Eui,
    data: &PushData,
    req: &RequestJoin,
    gw: LoRaGate,
) -> DeviceResult {
    let mut redis_conn = RedisClient::get_client().get_multiplexed_conn().await?;
    let info = match NodeInfo::load_by_eui(dev_eui, &mut redis_conn).await? {
        None => {
            let (node, devices) = DeviceLoraNodeEntity::find()
                .filter(DeviceLoraNodeColumn::DevEui.eq(dev_eui))
                .find_also_related(DevicesEntity)
                .one(&GLOBAL_STATE.db)
                .await?
                .ok_or_else(|| {
                    warn!("device eui({}) is not register", dev_eui);
                })?;
            if devices.is_none() {
                error!("dev_eui({}) in lora_node, but found in devices", dev_eui);
                return Err(DeviceError::Empty);
            }
            NodeInfo::register_to_redis(node, devices.unwrap(), &mut redis_conn).await?
        }
        Some(info) => info,
    };
    if info.join_type == LoRaJoinType::ABP {
        warn!("device not is otaa device");
        return Ok(())
    }
    if info.app_eui != app_eui {
        warn!("device app eui mismatch");
        return Ok(())
    }
    LoRaNodeManager::new_otaa_node(data, info, req.dev_nonce(), gw).await?;
    Ok(())
}

#[instrument(skip(payload, data, gw))]
async fn decode_enc_payload(
    payload: LoRaPayload,
    dev_addr: LoRaAddr,
    up_count: u16,
    data: PushData,
    gw: LoRaGate,
) -> DeviceResult {
    let mut node = LoRaNodeManager::get_node_with_gateway(dev_addr, gw).await?;
    
    if up_count < 5 {
        let otaa_info = node.get_otaa_info().await?;
        if let Some(otaa_info) = otaa_info {
            match payload.decrypt_mic(&otaa_info.nwk_skey, &otaa_info.app_skey, up_count as u32) {
                Ok(_) => {
                    let db_info = DeviceLoraNodeEntity::find()
                        .filter(DeviceLoraNodeColumn::DeviceId.eq(node.info.device_id))
                        .one(&GLOBAL_STATE.db)
                        .await?
                        .ok_or_else(|| {
                            warn!("device({}) eui({}) is delete", node.info.device_id, node.info.dev_eui);
                        })?;
                    let mut active_model = db_info.into_active_model();
                    active_model.nwk_skey = ActiveValue::Set(otaa_info.nwk_skey);
                    active_model.app_skey = ActiveValue::Set(otaa_info.app_skey);
                    active_model.dev_non = ActiveValue::Set(otaa_info.dev_nonce as i32);
                    active_model.net_id = ActiveValue::Set(otaa_info.net_id as _);
                    active_model.app_non = ActiveValue::Set(otaa_info.app_nonce as _);
                    active_model.update(&GLOBAL_STATE.db).await?;
                    let mut conn = RedisClient::get_client().get_multiplexed_conn().await?;
                    node.info.nwk_skey = otaa_info.nwk_skey;
                    node.info.app_skey = otaa_info.app_skey;
                    node.info.dev_non = otaa_info.dev_nonce as i32;
                    node.info.net_id = otaa_info.net_id as i32;
                    node.info.app_non = otaa_info.app_nonce as i32;
                    
                    redis::cmd("HSET")
                        .arg(&node.key)
                        .arg(NodeInfo::nwk_skey())
                        .arg(node.info.nwk_skey)
                        .arg(NodeInfo::app_skey())
                        .arg(node.info.app_skey)
                        .arg(NodeInfo::dev_non())
                        .arg(node.info.dev_non)
                        .arg(NodeInfo::net_id())
                        .arg(node.info.net_id)
                        .arg(NodeInfo::app_non())
                        .arg(node.info.app_non)
                        .arg(NodeInfo::up_count())
                        .arg(up_count)
                        .arg(NodeInfo::down_count())
                        .arg(up_count)
                        .exec_async(&mut conn)
                        .await?;
                }
                Err(_) => {
                    warn!("otaa join decrypt mic failed");
                    return Err(DeviceError::Empty)
                }
            }
        }
    }

    decode_node_payload(
        payload.f_port(),
        payload,
        node.info.dev_eui,
        node.info.device_id,
        node,
        up_count,
        data,
    )
        .await?;
    Ok(())
}

#[instrument(skip(payload, data, node, up_count))]
async fn decode_node_payload(
    f_port: Option<u8>,
    payload: LoRaPayload,
    dev_eui: Eui,
    device_id: Id,
    mut node: LoRaNode,
    up_count: u16,
    data: PushData,
) -> DeviceResult {
    let up_count_pre = node.info.up_count as u16;
    if up_count_pre == up_count && up_count != 0 {
        tracing::info!("repetition payload");
        Ok(())
    } else {
        tracing::info!("decode payload");
        let fmp = payload_decode(&mut node, &payload, up_count).await?;

        let s = DATA_QUEUE.insert(node.info.dev_addr, data, payload, fmp);
        if s {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let datas = DATA_QUEUE.get(node.info.dev_addr);
            match datas {
                Some(d) => {
                    decode_payload(
                        &d.push,
                        &mut node,
                        &d.payload,
                        d.data,
                    )
                        .await?;
                }
                None => {
                    warn!("no datas");
                }
            }
        } else {
            info!("repetition lora payload");
        }
        Ok(())
    }
}



async fn payload_decode(node: &mut LoRaNode, payload: &LoRaPayload, current_up_count: u16)
  -> DeviceResult<DecryptedDataPayload<Vec<u8>>>
{
    
    let pre_count = node.info.up_count as u16;
    let mut new_up_count = node.info.up_count;
    let up_count_diff = current_up_count.wrapping_sub(pre_count) as u32;
    info!("pre_count: {}, count: {}, up_count_diff: {}", pre_count, current_up_count, up_count_diff );
    let decode = if up_count_diff < ( 1 << 15 ) {
        new_up_count = new_up_count.wrapping_add(up_count_diff);
        payload.decrypt_mic(&node.info.nwk_skey, &node.info.app_skey, new_up_count)
    } else {
        new_up_count = new_up_count.wrapping_add(0x10000).wrapping_add(up_count_diff);
        payload.decrypt_mic(&node.info.nwk_skey, &node.info.app_skey, new_up_count)
    };
    if let Ok(o) = decode {
        node.update_up_count(new_up_count).await?;
        return Ok(o)
    }
    // ABP device reset
    let payload = payload.decrypt_mic(&node.info.nwk_skey, &node.info.app_skey, current_up_count as u32)?;
    node.update_up_count(current_up_count as u32).await?;
    node.reset_down_count().await?;
    Ok(payload)
}
