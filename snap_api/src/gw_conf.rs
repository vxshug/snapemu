use serde::{Deserialize, Serialize};


#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GwConfig {
    #[serde(rename = "SX130x_conf")]
    pub sx130x_conf: Sx130xConf,
    #[serde(rename = "gateway_conf")]
    pub gateway_conf: GatewayConf,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sx130xConf {
    #[serde(rename = "com_type")]
    pub com_type: String,
    #[serde(rename = "com_path")]
    pub com_path: String,
    #[serde(rename = "lorawan_public")]
    pub lorawan_public: bool,
    pub clksrc: i64,
    pub region: String,
    pub subband: i64,
    #[serde(rename = "antenna_gain")]
    pub antenna_gain: i64,
    #[serde(rename = "full_duplex")]
    pub full_duplex: bool,
    #[serde(rename = "precision_timestamp")]
    pub precision_timestamp: PrecisionTimestamp,
    #[serde(rename = "radio_0")]
    pub radio_0: Radio,
    #[serde(rename = "radio_1")]
    pub radio_1: Radio,
    #[serde(rename = "chan_multiSF_0")]
    pub chan_multi_sf_0: ChanMultiSf,
    #[serde(rename = "chan_multiSF_1")]
    pub chan_multi_sf_1: ChanMultiSf,
    #[serde(rename = "chan_multiSF_2")]
    pub chan_multi_sf_2: ChanMultiSf,
    #[serde(rename = "chan_multiSF_3")]
    pub chan_multi_sf_3: ChanMultiSf,
    #[serde(rename = "chan_multiSF_4")]
    pub chan_multi_sf_4: ChanMultiSf,
    #[serde(rename = "chan_multiSF_5")]
    pub chan_multi_sf_5: ChanMultiSf,
    #[serde(rename = "chan_multiSF_6")]
    pub chan_multi_sf_6: ChanMultiSf,
    #[serde(rename = "chan_multiSF_7")]
    pub chan_multi_sf_7: ChanMultiSf,
    #[serde(rename = "chan_Lora_std")]
    pub chan_lora_std: ChanLoraStd,
    #[serde(rename = "chan_FSK")]
    pub chan_fsk: ChanFsk,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrecisionTimestamp {
    pub enable: bool,
    #[serde(rename = "max_ts_metrics")]
    pub max_ts_metrics: i64,
    #[serde(rename = "nb_symbols")]
    pub nb_symbols: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Radio {
    pub enable: bool,
    #[serde(rename = "type")]
    pub type_field: String,
    pub freq: i64,
    #[serde(rename = "rssi_offset")]
    pub rssi_offset: f64,
    #[serde(rename = "rssi_tcomp")]
    pub rssi_tcomp: RssiTcomp,
    #[serde(rename = "tx_enable")]
    pub tx_enable: bool,
    #[serde(rename = "tx_freq_min")]
    pub tx_freq_min: Option<i64>,
    #[serde(rename = "tx_freq_max")]
    pub tx_freq_max: Option<i64>,
    #[serde(rename = "tx_gain_lut")]
    pub tx_gain_lut: Option<Vec<TxGainLut>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RssiTcomp {
    #[serde(rename = "coeff_a")]
    pub coeff_a: i64,
    #[serde(rename = "coeff_b")]
    pub coeff_b: i64,
    #[serde(rename = "coeff_c")]
    pub coeff_c: f64,
    #[serde(rename = "coeff_d")]
    pub coeff_d: f64,
    #[serde(rename = "coeff_e")]
    pub coeff_e: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxGainLut {
    #[serde(rename = "rf_power")]
    pub rf_power: i64,
    #[serde(rename = "pa_gain")]
    pub pa_gain: i64,
    #[serde(rename = "pwr_idx")]
    pub pwr_idx: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChanMultiSf {
    pub enable: bool,
    pub radio: i64,
    #[serde(rename = "if")]
    pub if_field: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChanLoraStd {
    pub enable: bool,
    pub radio: i64,
    #[serde(rename = "if")]
    pub if_field: i64,
    pub bandwidth: i64,
    #[serde(rename = "spread_factor")]
    pub spread_factor: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChanFsk {
    pub enable: bool,
    pub radio: i64,
    #[serde(rename = "if")]
    pub if_field: i64,
    pub bandwidth: i64,
    pub datarate: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayConf {
    #[serde(rename = "gateway_ID")]
    pub gateway_id: String,
    #[serde(rename = "server_address")]
    pub server_address: String,
    #[serde(rename = "serv_port_up")]
    pub serv_port_up: i64,
    #[serde(rename = "serv_port_down")]
    pub serv_port_down: i64,
    #[serde(rename = "keepalive_interval")]
    pub keepalive_interval: i64,
    #[serde(rename = "stat_interval")]
    pub stat_interval: i64,
    #[serde(rename = "push_timeout_ms")]
    pub push_timeout_ms: i64,
    #[serde(rename = "forward_crc_valid")]
    pub forward_crc_valid: bool,
    #[serde(rename = "forward_crc_error")]
    pub forward_crc_error: bool,
    #[serde(rename = "forward_crc_disabled")]
    pub forward_crc_disabled: bool,
}