export interface DeviceEventRes {
  device: string,
  event: JoinRequestEvent | JoinAcceptEvent | UplinkDataEvent | DownLinkDataEvent | GatewayDataEvent
}

export interface JoinRequestEvent {
  event: 'JoinRequest',
  app_eui: string,
  dev_eui: string,
  time: number
}


export interface JoinAcceptEvent {
  event: 'JoinAccept',
  dev_addr: string,
  time: number
}

export interface UplinkDataEvent {
  event: 'UplinkData',
  confirm: boolean,
  dev_addr: string,
  f_cnt: number,
  f_port: number,
  payload: string,
  decoded_payload: string,
  time: number,
  gateway: {
    eui: string,
    id: string,
    rssi: number,
    snr: number,
    time: number
  }
}
export interface DownLinkDataEvent {
  event: 'DownLinkData',
  confirm: boolean,
  f_port: number,
  bytes: string,
  time: number
}

export interface GatewayDataEvent {
  event: 'Gateway',
  eui: string,
  source: {
    ip: string
  },
  time: number,
  gateway_event: GatewayJoinData | GatewayData | GatewayStatus
}

export interface GatewayJoinData {
  type: "Join",
  app_eui: string,
  dev_eui: string,
  dev_nonce: string,
}

export interface GatewayData {
  type: "Data",
  payload: string,
  f_port: number,
  f_cnt: number,
  dev_addr: string,
  datr: string,
  codr: string,
  frequency: number,
  rssi: number,
  snr: number,
  channel: number
}

export interface GatewayStatus {
  type: "Status",
  time?: string,
  lati?: number,
  long?: number,
  alti?: number,
  rxnb?: number,
  rxok?: number,
  rwfw?: number,
  ackr?: number,
  dwnb?: number,
  txnb?: number
}

