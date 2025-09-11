use common_define::decode::Value;
use influxdb2_structmap::GenericMap;

pub(crate) struct DataService;

pub(crate) mod query;
pub(crate) mod update;

#[derive(Debug, Default)]
pub struct DeviceData {
    id: i64,
    name: String,
    unit: String,
    value: Value,
    time: i64,
}

impl influxdb2::FromMap for DeviceData {
    fn from_genericmap(map: GenericMap) -> Self {
        let mut map = map;
        let id = if let Some(influxdb2_structmap::value::Value::String(id)) = map.remove("id") {
            id.parse::<i64>().unwrap_or_default()
        } else {
            0
        };
        let name = if let Some(influxdb2_structmap::value::Value::String(name)) = map.remove("name")
        {
            name
        } else {
            String::new()
        };
        let unit = if let Some(influxdb2_structmap::value::Value::String(unit)) = map.remove("unit")
        {
            unit
        } else {
            String::new()
        };
        let value = map
            .remove("value")
            .or_else(|| map.remove("f_value"))
            .or_else(|| map.remove("b_value"))
            .unwrap_or(influxdb2_structmap::value::Value::Long(0));
        let value = match value {
            influxdb2_structmap::value::Value::Long(i) => Value::Int(i),
            influxdb2_structmap::value::Value::Double(f) => Value::Float(f.0),
            influxdb2_structmap::value::Value::Bool(b) => Value::Bool(b),
            _ => Value::default(),
        };
        let time =
            if let Some(influxdb2_structmap::value::Value::TimeRFC(time)) = map.remove("_time") {
                time.timestamp_millis()
            } else {
                0
            };
        Self { id, name, unit, value, time }
    }
}

