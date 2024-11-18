import { cppFile, dataType } from '@/utils/define'

function mapParams(name: string, dType: string) {
  switch (dType) {
    case dataType.I8:
      return `int8_t ${name}`
    case dataType.U8:
      return `uint8_t ${name}`
    case dataType.I16:
      return `int16_t ${name}`
    case dataType.U16:
      return `uint16_t ${name}`
    case dataType.I32:
      return `int32_t ${name}`
    case dataType.U32:
      return `uint32_t ${name}`
    case dataType.F32:
      return `float ${name}`
    case dataType.F64:
      return `double ${name}`
    case dataType.Bool:
      return `bool ${name}`
  }
  throw new Error(`Unknown type "${dType}"`)
}

function packetDataLine(dataName: string,  dType: string, dataId: number) {
  switch (dType) {
    case dataType.I8:
      return `if ((output_data_len_max + 2) < offset)
    {
        return -1;
    }
    ret = int8_t_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.U8:
      return `if ((output_data_len_max + 2) < offset)
    {
        return -1;
    }
    ret = uint8_t_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.I16:
      return `if ((output_data_len_max + 3) < offset)
    {
        return -1;
    }
    ret = int16_t_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.U16:
      return `if ((output_data_len_max + 3) < offset)
    {
        return -1;
    }
    ret = uint16_t_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.I32:
      return `if ((output_data_len_max + 5) < offset)
    {
        return -1;
    }
    ret = int32_t_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.U32:
      return `if ((output_data_len_max + 5) < offset)
    {
        return -1;
    }
    ret = uint32_t_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.F32:
      return `if ((output_data_len_max + 5) < offset)
    {
        return -1;
    }
    ret = float_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.F64:
      return `if ((output_data_len_max + 9) < offset)
    {
        return -1;
    }
    ret = double_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
    case dataType.Bool:
      return `if ((output_data_len_max + 2) < offset)
    {
        return -1;
    }
    ret = bool_type_data_packet(${dataId}, ${dataName}, output_data + offset);
    `
  }
  throw new Error(`Unknown type "${dType}"`)
}

function parseMap(map: {
  d_id: number,
  d_name: string,
  d_type: string,
  d_unit: string,
}[]) {
  const params = []
  const bodyMap = new Map<string, string[]>()
  for (const item of map) {
    params.push(mapParams(item.d_name, item.d_type))
    const sensorId = item.d_id / 16;
    const dataId = item.d_id % 16;
    const enumItem = "CUSTOM_PARENT_ID" + sensorId.toString(16).toUpperCase()
    const code = packetDataLine(item.d_name, item.d_type, dataId);
    const arr = bodyMap.get(enumItem);
    if (arr === undefined) {
      bodyMap.set(enumItem, [code])
    } else {
      arr.push(code)
    }
  }
  return bodyMap
}

function generateCode(map: Map<string, string[]>) {
  let code = '';
  for (const [sensor, packets] of map) {
    code = `${code}
    offset += 3;
    if (output_data_len_max < offset)
    {
        return -1;
    }
    sensor_index = offset - 1;
    parent_id_packet(${sensor}, output_data + offset -3);
     `
    for (const packet of packets) {
      code = `${code}
    ${packet}
    if (ret < 0)
    {
        return -1;
    }
    offset += ret;
     `
    }
    code = `${code}
    output_data[sensor_index] = offset - sensor_index - 1;
    `
  }
  return code;
}

export function cppcode(battery: boolean, charging: boolean, map: {
  d_id: number,
  d_name: string,
  d_type: string,
  d_unit: string,
}[]) {
  const params = map.map(item => mapParams(item.d_name, item.d_type) ).join(", ");
  const codeMap = parseMap(map);
  const codeBody = generateCode(codeMap);
  return `
${cppFile}

address_offset_t sensor_data_packet(uint8_t* output_data, uint16_t output_data_len_max ${battery ? ', uint8_t battery': '' } ${charging ? ', bool charging' : ''}, ${params})
{
    #define SENSOR_ID 1
    address_offset_t offset = 0;
    address_offset_t ret;
    address_offset_t sensor_index = 0;
    if (output_data == NULL)
    {
        return -1;
    }
    ${ (battery || battery) ? `offset += 3;
    if (output_data_len_max < offset)
    {
        return -1;
    }
    sensor_index = offset - 1;` : '' }
    ${ battery  ? `parent_id_packet(BATTERY_VOLTAGE_PARENT_ID, output_data);
    ret = uint8_t_type_data_packet(BATTERY_LEVEL_CHILD_ID, battery, output_data + offset);
    if (ret < 0)
    {
        return -1;
    }
    offset += ret;` : '' }
    ${ charging  ? `ret = bool_type_data_packet(BATTERY_CHARGING_STATE_CHILD_ID, charging, output_data + offset);
    if (ret < 0)
    {
        return -1;
    }
    offset += ret;` : '' }
    ${ (battery || battery) ? 'output_data[sensor_index] = offset - sensor_index - 1;' : '' }
    ${codeBody}
    return offset;
}
`
}