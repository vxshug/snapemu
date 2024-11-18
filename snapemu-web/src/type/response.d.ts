export type LoRaRegion = 'EU868'| 'US915'| 'CN779'| 'EU433' | 'AU915'| 'CN470'| 'AS923'| 'AS923_2'| 'AS923_3'| 'KR920'| 'IN865'| 'RU864';
export type DeviceDataType =  'Array'| 'F64'| 'F32'| 'Bool'| 'I8'| 'U8'| 'I16'| 'U16'| 'I32'| 'U32';
export type DeviceType = "LoRaNode" | "LoRaGate";


export interface UserToken {
  access_token: string
}

export interface OffsetResp {
  offset: number
}

export interface DevicesResp<T> extends OffsetResp {
  device_count: number
  devices: T[]
}


export interface DeviceGroupResp {
  id: string
  name: string
  device_count: number
  default_group: boolean
  offset: number
  description: string
  devices: GroupDevice[]
}

export interface DeviceGroup {
  id: string
  name: string
  description: string
  device_count: number
  default_group: boolean
}

export interface DeviceGroupBody {
  name: string,
  description: string
}

export interface DeviceAllResp {
  device_count: number
  offset: number
  devices: GroupDevice[]
}

export interface GroupDevice {
  id: string
  name: string
  online: boolean
  battery?: number
  data: DeviceOneData[]
  description: string
  device_type: DeviceType
  active_time?: number
}

export interface DeviceOneData {
  name: string
  counts: number
  data_id: number
  unit: string
  v_type: DeviceDataType
  data: TimeData
}
export interface DeviceSource {
  share_type: string
  owner: boolean
  manager: boolean
  modify: boolean
  delete: boolean
  share: boolean
}
export interface LoRaNodeInfo {
  region: LoRaRegion
  join_type: string
  app_eui?: string
  dev_eui?: string
  app_key?: string
  dev_addr?: string
  nwk_skey?: string
  app_skey?: string
  class_b: boolean
  class_c: boolean
  adr: boolean
  rx1_delay: number
  des_rx1_delay: number
  rx1_dro: number
  des_rx1_dro: number
  rx2_dr: number
  des_rx2_dr: number
  rx2_freq: number
  des_rx2_freq: number
  d_retry: number
  c_retry: number
  dutycyle: number
  product_type: string
  up_confirm?: number
  up_dr?: number
  power?: number
  battery: number
  charge: boolean
  time_zone?: number
  firmware?: number
  dev_non?: number
  app_non?: number
  net_id?: number
}

export interface LoRaGateInfo {
  device_id: string
  region: string
  eui: string
}
export type DeviceInfo = LoRaNodeInfo | LoRaGateInfo;

export interface DeviceResp {
  id: string
  name: string
  online: boolean
  description: string
  info: DeviceInfo
  device_type: DeviceType
  source: DeviceSource
  script?: string
  create_time: string
  active_time: number
}


export interface DeviceData {
  name: string
  counts: number
  data_id: number
  unit: string
  v_type: DeviceDataType
  data: TimeData[]
}

export interface TimeData {
  time: number
  data: any
}
export interface DataResp {
  counts: number
  offset: number
  data: DeviceData[]
}

export interface UserInfo {
  username: string,
  picture: string,
  email: string,
}

export interface UserPutInfo {
  password?: string,
  old_password?: string,
}

export interface ScriptMap {
  d_name: string,
  d_unit: string,
  d_type: string,
  d_id: number,
}

export interface Script {
  id: string,
  name: string,
  script: string,
  lang: 'JS',
  map: ScriptMap[]
}

export interface TestScript {
  script: string,
  bytes: string,
  lang: 'JS'
}

export interface TestScriptResult {
  result: string,
  state: boolean,
}

export interface IntegrationToken {
  name: string,
  enable: boolean,
  username: string,
  password: string,
  client_id: string,
  create_time: number
}

export interface IntegrationResp {
  count: number,
  tokens: IntegrationToken[]
}

export interface IntegrationBody {
  group: boolean,
  name: string,
  device: number
}

export type ShowDevice = DevicesResp<Pick<DeviceResp, 'id' | 'name' | 'create_time' | 'active_time'>>