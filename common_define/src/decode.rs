use base64::Engine;
use derive_new::new;
use crate::time::Timestamp;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, strum::AsRefStr, strum::EnumString,  redis_macros::FromRedisValue, redis_macros::ToRedisArgs)]
pub enum DecodeLang {
    JS,
}


#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, strum::AsRefStr, strum::EnumString, Eq, PartialEq)]
pub enum DecodeDataType {
    I32,
    F64,
    Bool
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, strum::AsRefStr, strum::EnumString, Eq, PartialEq)]
pub enum CustomDecodeDataType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    F32,
    F64,
    Bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, new, redis_macros::FromRedisValue, redis_macros::ToRedisArgs)]
pub struct LastDecodeData {
    pub v: Vec<DecodeData>,
    pub t: Timestamp
}


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, new, redis_macros::FromRedisValue, redis_macros::ToRedisArgs)]
pub struct DecodeData {
    pub i: u32,
    pub v: Value
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            _ => false,
        }
    }
}
impl Eq for Value {}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(try_from = "&str")]
pub struct Array(Vec<u8>);
impl TryFrom<&str> for Array {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        base64::engine::general_purpose::STANDARD.decode(value)
            .map(Self)
            .map_err(|e|e.to_string())
    }
}

macro_rules! value_from {
    ($t:ty, $i:ty, $f:expr) => {
        impl From<$t> for Value {
            fn from(value: $t) -> Self {
                $f(value as $i)
            }
        }
    }
}

value_from!(i8, i64, Value::Int);
value_from!(u8, i64, Value::Int);
value_from!(i16, i64, Value::Int);
value_from!(u16, i64, Value::Int);
value_from!(i32, i64, Value::Int);
value_from!(u32, i64, Value::Int);

value_from!(bool, bool, Value::Bool);
value_from!(f32, f64, Value::Float);
value_from!(f64, f64, Value::Float);


