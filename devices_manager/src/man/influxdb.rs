use crate::db::DataError;
use crate::decode::DecodeData;
use chrono::Utc;
use common_define::Id;
use derive_new::new;
use influxdb2::models::data_point::DataPointError;
use influxdb2::models::DataPoint;
use influxdb2::RequestError;

#[derive(Debug, thiserror::Error)]
pub enum InfluxError {
    #[error("RequestError {0}")]
    Request(#[from] RequestError),
    #[error("DataError {0}")]
    Data(#[from] DataError),
    #[error("DataError {0}")]
    DataPoint(#[from] DataPointError),
}

#[derive(new)]
pub struct InfluxDbClient {
    bucket: String,
    client: influxdb2::Client,
}

impl InfluxDbClient {
    pub async fn write_js(&self, data: DecodeData, device_id: Id) -> Result<(), InfluxError> {
        let timestamp = Utc::now().timestamp_nanos_opt().ok_or(DataError::Time)?;
        let mut v = Vec::new();
        for item in data.data {
            let name = if item.name.is_empty() { " ".to_string() } else { item.name };
            let unit = if item.unit.is_empty() { " ".to_string() } else { item.unit };
            let data = match item.v {
                common_define::decode::Value::Int(i) => DataPoint::builder(device_id.to_string())
                    .tag("id", item.i.to_string())
                    .tag("name", name)
                    .tag("unit", unit)
                    .field("value", i)
                    .timestamp(timestamp)
                    .build()?,
                common_define::decode::Value::Float(f) => DataPoint::builder(device_id.to_string())
                    .tag("id", item.i.to_string())
                    .tag("name", name)
                    .tag("unit", unit)
                    .field("f_value", f)
                    .timestamp(timestamp)
                    .build()?,
                common_define::decode::Value::Bool(b) => DataPoint::builder(device_id.to_string())
                    .tag("id", item.i.to_string())
                    .tag("name", name)
                    .tag("unit", unit)
                    .field("b_value", b)
                    .timestamp(timestamp)
                    .build()?,
            };
            v.push(data);
        }
        Ok(self.client.write(&self.bucket, tokio_stream::iter(v)).await?)
    }
}
