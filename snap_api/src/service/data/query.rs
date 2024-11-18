use std::collections::{BTreeMap, HashMap};
use std::ops::Sub;
use crate::error::{ApiResult};
use crate::service::data::DataService;
use crate::{get_lang, AppState, MODEL_MAP};

use derive_new::new;
use redis::AsyncCommands;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};
use common_define::db::{DecodeScriptEntity, DeviceDataColumn, DeviceDataEntity, DevicesModel};
use common_define::decode::{LastDecodeData, Value};
use common_define::{last_device_data_key, Id};
use common_define::product::DeviceType;
use common_define::time::Timestamp;

#[derive(Deserialize, Serialize, Clone, new)]
pub(crate) struct TimeDate {
    pub time: Timestamp,
    pub data: Value,
}
#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct DataResponse {
    pub name: String,
    pub counts: i32,
    pub data_id: u32,
    pub unit: String,
    pub data: Vec<TimeDate>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct DataDeviceOneResponse {
    pub(crate) name: String,
    pub(crate) data_id: u32,
    pub(crate) unit: String,
    pub(crate) data: TimeDate,
}
#[derive(Deserialize, Serialize, Clone, redis_macros::FromRedisValue, redis_macros::ToRedisArgs)]
pub(crate) struct DataDeviceOneResponseWrap {
    pub counts: i64,
    pub data: Vec<DataDeviceOneResponse>,
    pub update: Timestamp
}


#[derive(Deserialize, Serialize, Clone, redis_macros::FromRedisValue, redis_macros::ToRedisArgs)]
pub(crate) struct DataResponseWrap {
    pub counts: i64,
    pub data: Vec<DataResponse>,
    pub update: Timestamp
}

#[derive(Copy, Clone, Debug)]
pub enum DataDuration {
    Hour,
    Day,
    Week
}


impl  DataDuration {
    fn duration(&self) -> chrono::Duration {
        match self {
            DataDuration::Hour => { chrono::Duration::hours(1) }
            DataDuration::Day => { chrono::Duration::days(1) }
            DataDuration::Week => { chrono::Duration::weeks(1) }
        }
    }
}

impl Sub<DataDuration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: DataDuration) -> Self::Output {
        match rhs {
            DataDuration::Hour => { self - chrono::Duration::hours(1) }
            DataDuration::Day => { self - chrono::Duration::days(1) }
            DataDuration::Week => { self - chrono::Duration::weeks(1) }
        }
    }
}

impl DataService {
    // pub(crate) async fn query_device(
    //     user: &CurrentUser,
    //     device: Id,
    //     hour: i64,
    //     conn: &mut DBConnection,
    // ) -> ApiResult<DataResponseWrap> {
    //     let time = Utc::now() - Duration::hours(hour);
    //     let data_all: Vec<DeviceData> = DBController::query(
    //         sqlx::query_as(DeviceData::SELECT_BY_DEVICE_ID_AND_TIME)
    //             .bind(device)
    //             .bind(time)
    //             .fetch_all(conn.as_mut()),
    //     ).await?;
    //     let mut data_map: BTreeMap<u32, Vec<TimeDate>> = BTreeMap::new();
    //     
    //     
    //     
    //     for data in data_all {
    //         for d in data.data.0 {
    //             if let Some(data_vec) = data_map.get_mut(&d.i) {
    //                 data_vec.push(TimeDate::new(data.time.clone(), d.v))
    //             } else {
    //                 data_map.insert(d.i, vec![TimeDate::new(data.time.clone(), d.v)]);
    //             }
    //         }
    //     };
    // 
    //     let models: &ModelMap = GLOBAL_DEP.get_ref();
    // 
    // 
    //     let mut resp = vec![];
    //     for (data_id, device_data) in data_map {
    //         let data_name = models.get_entry(data_id as u32, user.lang.as_ref());
    // 
    // 
    //         let data = DataResponse {
    //             name: data_name.name.to_string(),
    //             counts: device_data.len() as i32,
    //             data_id,
    //             unit: data_name.unit.to_string(),
    //             data: device_data,
    //         };
    //         resp.push(data)
    //     };
    //     Ok(DataResponseWrap {
    //         counts: resp.len() as i64,
    //         data: resp
    //     })
    // }

    // pub(crate) async fn query_data(
    //     user: &CurrentUser,
    //     device: Id,
    //     data_id: Option<i32>,
    //     hour: i64,
    //     conn: &mut DBConnection,
    // ) -> ApiResult<DataResponseWrap> {
    //     let time = Utc::now() - Duration::hours(hour);
    //     let mut data_ids: HashSet<i32> = HashSet::new();
    //     let data_all: Vec<DeviceData> = match data_id {
    //         None => {
    //             let data: Vec<DeviceData> = DBController::query(
    //                 sqlx::query_as(DeviceData::SELECT_ALL_BY_DEVICE_ID_AND_TIME)
    //                     .bind(device)
    //                     .bind(time)
    //                     .fetch_all(conn.as_mut()),
    //             ).await?;
    //             for datum in &data {
    //                 data_ids.insert(datum.data_id);
    //             }
    //             data
    //         }
    //         Some(data_id) => {
    //             data_ids.insert(data_id);
    //             DBController::query(
    //                 sqlx::query_as(DeviceData::SELECT_ALL_BY_DEVICE_ID_AND_TIME_DATA_ID)
    //                     .bind(device)
    //                     .bind(data_id)
    //                     .bind(time)
    //                     .fetch_all(conn.as_mut()),
    //             ).await?
    //         }
    //     };
    // 
    //     let models: &ModelMap = GLOBAL_DEP.get_ref();
    //     let mut data_map: BTreeMap<i32, DataResponse> = BTreeMap::new();
    // 
    //     for data in data_all {
    //         match data_map.get_mut(&data.data_id) {
    //             Some(d) => {
    //                 d.counts += 1;
    //                 d.data.push(
    //                     TimeDate {
    //                         time: data.time,
    //                         data: data.data.0
    //                     }
    //                 )
    //             }
    //             None => {
    //                 let data_name = models.get_entry(data.data_id as u32, user.lang.as_ref());
    //                 let res=
    //                     DataResponse {
    //                         name: data_name.name.to_string(),
    //                         counts: 1,
    //                         data_id: data.data_id,
    //                         unit: data_name.unit.to_string(),
    //                         v_type: data.v_type,
    //                         data: vec![
    //                             TimeDate {
    //                                 time: data.time,
    //                                 data: data.data.0,
    //                             }
    //                         ],
    //                     };
    //                 data_map.insert(data.data_id, res);
    //             }
    //         }
    //     }
    //     let mut data: Vec<DataResponse> = data_map.into_values().collect();
    //     data.sort_by(|pre, cur| pre.data_id.cmp(&cur.data_id));
    // 
    //     Ok(DataResponseWrap {
    //         counts: data.len() as i64,
    //         data,
    //     })
    // }
    
    fn device_duration_key(device: Id, data_duration: DataDuration) -> String {
        let lang = get_lang().as_static_str();
        match data_duration {
            DataDuration::Hour => { format!("data:hour:{}:{}", lang, device) }
            DataDuration::Day => { format!("data:day:{}:{}", lang, device) }
            DataDuration::Week => { format!("data:week:{}:{}", lang, device) }
        }
    }

    fn device_last_key(device: Id) -> String {
        let lang = get_lang().as_static_str();
        format!("data:last:{}:{}", lang, device)
    }
    pub(crate) async fn query_duration_data(
        device: Id,
        script_id: Option<Id>,
        data_duration: DataDuration,
        state: &AppState,
    ) -> ApiResult<DataResponseWrap> {
        let key = Self::device_duration_key(device, data_duration);
        let lang = get_lang().as_static_str();
        let mut redis_conn = state.redis.get().await?;
        let data_resp: Option<DataResponseWrap> = redis_conn.get(&key).await?;
        if let Some(data) = data_resp {
            if data.update.timestamp_millis() > Timestamp::now().timestamp_millis() + 10000 {
                return Ok(data)
            }
        }
        
        let start = Timestamp::now() - data_duration;
        let conn = &state.db;

        let data_all = DeviceDataEntity::find()
            .filter(DeviceDataColumn::DeviceId.eq(device).and(DeviceDataColumn::CreateTime.gt(start)))
            .order_by_asc(DeviceDataColumn::Id)
            .all(conn)
            .await?;

        let mut data_map: BTreeMap<u32, DataResponse> = BTreeMap::new();
        
        match script_id {
            None => {
                for data in data_all {
                    for x in data.data.0 {
                        match data_map.get_mut(&x.i) {
                            Some(d) => {
                                d.counts += 1;
                                d.data.push(
                                    TimeDate {
                                        time: data.create_time,
                                        data: x.v
                                    }
                                )
                            }
                            None => {
                                let data_name = MODEL_MAP.get_entry(x.i, lang);
                                let res=
                                    DataResponse {
                                        name: data_name.name.to_string(),
                                        counts: 1,
                                        data_id: x.i,
                                        unit: data_name.unit.to_string(),
                                        data: vec![
                                            TimeDate {
                                                time: data.create_time,
                                                data: x.v,
                                            }
                                        ],
                                    };
                                data_map.insert(x.i, res);
                            }
                        }
                    }
                }
            }
            Some(script_id) => {
                let map = DecodeScriptEntity::find_by_id(script_id).one(conn).await?;
                if let Some(script) = map {
                    let map: HashMap<_, _> = script.map.iter().map(|it| (it.id, it)).collect();
                    for data in data_all {
                        for x in data.data.0 {
                            match data_map.get_mut(&x.i) {
                                Some(d) => {
                                    d.counts += 1;
                                    d.data.push(
                                        TimeDate {
                                            time: data.create_time,
                                            data: x.v
                                        }
                                    )
                                }
                                None => {
                                    if let Some(m) = map.get(&x.i) {
                                        let res=
                                            DataResponse {
                                                name: m.name.clone(),
                                                counts: 1,
                                                data_id: x.i,
                                                unit: m.unit.clone(),
                                                data: vec![
                                                    TimeDate {
                                                        time: data.create_time,
                                                        data: x.v,
                                                    }
                                                ],
                                            };
                                        data_map.insert(x.i, res);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut data: Vec<DataResponse> = data_map.into_values().collect();
        data.sort_by(|pre, cur| pre.data_id.cmp(&cur.data_id));

        let resp = DataResponseWrap {
            counts: data.len() as i64,
            data,
            update: Timestamp::now()
        };
        
        redis_conn.set(&key, &resp).await?;
        
        Ok(resp)
    }

    pub(crate) async fn query_last(
        device: &DevicesModel,
        state: &AppState,
    ) -> ApiResult<DataDeviceOneResponseWrap> {
        let script_id = device.script;
        if device.device_type == DeviceType::LoRaGate  { 
            return Ok(DataDeviceOneResponseWrap {
                counts: 0,
                data: vec![],
                update: Timestamp::now(),
            })
        }
        let conn = &state.db;
        let key = Self::device_last_key(device.id);
        let lang = get_lang().as_static_str();
        let mut redis_conn = state.redis.get().await?;
        let data_resp: Option<DataDeviceOneResponseWrap> = redis_conn.get(&key).await?;
        if let Some(data) = data_resp {
            if data.update.timestamp_millis() > Timestamp::now().timestamp_millis() + 10000 {
                return Ok(data)
            }
        }

        let mut redis_conn = state.redis.get().await?;
        let last_data: Option<LastDecodeData> = redis_conn.get(last_device_data_key(device.id)).await?;
        
        match last_data {
            None => {
                Ok(DataDeviceOneResponseWrap {
                    counts: 0,
                    data: vec![],
                    update: Timestamp::now(),
                })
            }
            Some(data) => {
                let mut resp = vec![];
                match script_id {
                    None => {
                        for d in data.v {
                            let data_name = MODEL_MAP.get_entry(d.i, lang);
                            let data = DataDeviceOneResponse {
                                name: data_name.name.to_string(),
                                data_id: d.i,
                                unit: data_name.unit.to_string(),
                                data: TimeDate {
                                    time: data.t,
                                    data: d.v
                                }
                            };
                            resp.push(data)
                        };
                    }
                    Some(id) => {
                        let map = DecodeScriptEntity::find_by_id(id).one(conn).await?;
                        match map {
                            None => {
                                return Ok(DataDeviceOneResponseWrap {
                                    counts: 0,
                                    data: vec![],
                                    update: Timestamp::now(),
                                })
                            }
                            Some(map) => {
                                let map: HashMap<_, _> = map.map.iter().map(|it| (it.id, it)).collect();
                                for d in data.v {
                                    if let Some(map) = map.get(&d.i) {
                                        let data = DataDeviceOneResponse {
                                            name: map.name.to_string(),
                                            data_id: d.i,
                                            unit: map.unit.to_string(),
                                            data: TimeDate {
                                                time: data.t,
                                                data: d.v
                                            }
                                        };
                                        resp.push(data)
                                    }
                                };
                            }
                        }
                    }
                }

                resp.sort_by(|pre, cur| pre.data_id.cmp(&cur.data_id));
                let resp = DataDeviceOneResponseWrap {
                    counts: resp.len() as i64,
                    data: resp,
                    update: Timestamp::now(),
                };

                redis_conn.set(&key, &resp).await?;
                Ok(resp)
            }
        }
    }
}
