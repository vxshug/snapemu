use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::decode::{DecodeData, RawData, JsDecodeError, JsManager};
use crate::man::Id;

pub struct DecodeModule {
    module: Vec<u8>,
    time: chrono::DateTime<chrono::Utc>
}

#[derive(Clone)]
pub struct DecodeManager {
    map: Arc<Mutex<HashMap<Id, DecodeModule>>>,
    rt: JsManager
}

impl DecodeManager {
    
    pub fn new(rt: JsManager) -> Self {
        Self {
            map: Default::default(),
            rt
        }
    }
    pub fn decode_with_id(&self, module_id: &Id, data: RawData) -> Option<Result<DecodeData, JsDecodeError>> {
        let map = self.map.lock().unwrap();
        let s = map.get(module_id)?;
        Some(self.rt.eval(s.module.as_slice(), data))
    }

    pub fn decode_with_code(&self, id: Id, code: &str, data: RawData) -> Result<DecodeData, JsDecodeError> {
        let module = self.rt.compile(code)?;
        let data = self.rt.eval(module.as_slice(), data)?;
        // self.map.lock().unwrap().insert(id, DecodeModule { module, time: chrono::Utc::now() });
        Ok(data)
    }
}