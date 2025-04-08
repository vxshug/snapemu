use serde::{Deserialize, Serialize};
use derive_new::new;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, new)]
#[serde(rename_all = "camelCase")]
pub struct GwCmd {
    #[serde(rename = "gw_type")]
    pub gw_type: String,
    #[serde(rename = "msg_type")]
    pub msg_type: GwMsgType,
    pub data: MsgData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, new)]
#[serde(rename_all = "snake_case")]
pub enum GwMsgType {
    ShellCmd,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, new)]
#[serde(rename_all = "camelCase")]
pub struct MsgData {
    #[serde(rename = "cmd_str")]
    pub cmd_str: String,
    #[serde(rename = "cmd_timeout_ms")]
    pub cmd_timeout_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, new)]
#[serde(rename_all = "camelCase")]
pub struct GwCmdResponse {
    #[serde(rename = "gw_type")]
    pub gw_type: String,
    #[serde(rename = "msg_type")]
    pub msg_type: GwMsgType,
    pub data: GwCmdResponseData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, new)]
#[serde(rename_all = "camelCase")]
pub struct GwCmdResponseData {
    #[serde(rename = "exc_result")]
    pub exc_result: String,
    #[serde(rename = "exc_result_code")]
    pub exc_result_code: i32,
}

