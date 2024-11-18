use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use common_define::event::DeviceEvent;
use common_define::Id;
use crate::error::{ApiError, ApiResult};

pub struct NodeEvent {
    rx: broadcast::Receiver<DeviceEvent>
}


impl NodeEvent {
    pub async fn event(&mut self) -> ApiResult<DeviceEvent> {
        self.rx.recv()
            .await
            .map_err(|e| ApiError::User("channel close".into()))
    }

    pub fn into_stream(self) -> BroadcastStream<DeviceEvent> {
        BroadcastStream::new(self.rx)
    }
}

#[derive(Clone)]
pub struct NodeEventManager {
    map: Arc<Mutex<HashMap<Id, broadcast::Sender<DeviceEvent>>>>
}

impl NodeEventManager {
    pub fn new() -> Self {
        Self {
            map: Default::default()
        }
    }

    pub(crate) fn subscribe(&self, device: Id) -> NodeEvent {
        let mut map = self.map.lock().unwrap();
        match map.get(&device) {
            None => {
                let (tx, rx) = broadcast::channel(10);
                map.insert(device, tx);
                NodeEvent {
                    rx
                }
            }
            Some(tx) => {
                NodeEvent {
                    rx: tx.subscribe()
                }
            }
        }
    }

    pub(crate) fn broadcast(&self, event: DeviceEvent)  {
        let mut map = self.map.lock().unwrap();
        match map.get(&event.device) {
            None => {}
            Some(tx) => {
                if let Err(e) = tx.send(event) {
                    map.remove(&e.0.device);
                }
            }
        }
    }
}



