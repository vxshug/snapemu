export const deviceDataCardBgColor = [
  "#038A89",
  "#A5D63F",
  "#9FBF5A",
  "#43CF7C",
  "#43CF7C",
  "#E6D96A",
  "#00BAAD",
  "#0A817F",
];

export const indexToBg = (index: number) => deviceDataCardBgColor[index % deviceDataCardBgColor.length];

export enum dataType {
  U8   = "U8",
  I8   = "I8",
  U16  = "U16",
  I16  = "I16",
  U32  = "U32",
  I32  = "I32",
  F32  = "F32",
  F64  = "F64",
  Bool = "Bool",
}


export const dataTypeArray = [
  dataType.U8,
  dataType.I8,
  dataType.U16,
  dataType.I16,
  dataType.U32,
  dataType.I32,
  dataType.F32,
  dataType.F64,
  dataType.Bool,
]

export const headerFile = `#ifndef __LORA_DATA_PACKET_H__
#define __LORA_DATA_PACKET_H__

#include "stdint.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SINGLE_SENSOR_DATA_PARENT_ID_FIELD_LEN (2)
#define SINGLE_SENSOR_DATA_LEN_FIELD_LEN       (1)

/*If address_offset_t is less than or equal to 0, it indicates a packaging error.*/
typedef int  address_offset_t;

#define BITS_8_TYPE_DATA_LEN     (1)
#define BITS_16_TYPE_DATA_LEN    (2)
#define BITS_32_TYPE_DATA_LEN    (4)
#define BITS_64_TYPE_DATA_LEN    (8)

typedef union
{
    double double_raw_data;
    uint8_t double_convert_data[BITS_64_TYPE_DATA_LEN];
}double_type_convert_t;

typedef union
{
    uint8_t uint8_t_raw_data;
    int8_t  int8_t_raw_data;
    bool    bool_raw_data;
    uint8_t bits_8_convert_data[BITS_8_TYPE_DATA_LEN];
}bits_8_type_convert_t;

typedef union
{
    uint16_t uint16_t_raw_data;
    int16_t  int16_t_raw_data;
    uint8_t bits_16_convert_data[BITS_16_TYPE_DATA_LEN];
}bits_16_type_convert_t;

typedef union
{
    uint32_t uint32_t_raw_data;
    int32_t  int32_t_raw_data;
    float    float_raw_data;
    uint8_t bits_32_convert_data[BITS_32_TYPE_DATA_LEN];
}bits_32_type_convert_t;

address_offset_t bool_type_data_packet(uint8_t sensor_data_id, bool sensor_data, uint8_t *output_data);
address_offset_t int8_t_type_data_packet(uint8_t sensor_data_id, int8_t sensor_data, uint8_t *output_data);
address_offset_t uint8_t_type_data_packet(uint8_t sensor_data_id, uint8_t sensor_data, uint8_t *output_data);
address_offset_t int16_t_type_data_packet(uint8_t sensor_data_id, int16_t sensor_data, uint8_t *output_data);
address_offset_t uint16_t_type_data_packet(uint8_t sensor_data_id, uint16_t sensor_data, uint8_t *output_data);

address_offset_t array_type_data_packet(uint8_t sensor_data_id, uint8_t* sensor_data, uint8_t sensor_data_len, uint8_t *output_data);
address_offset_t float_type_data_packet(uint8_t sensor_data_id, float sensor_data, uint8_t *output_data);
address_offset_t int32_t_type_data_packet(uint8_t sensor_data_id, int32_t sensor_data, uint8_t *output_data);
address_offset_t uint32_t_type_data_packet(uint8_t sensor_data_id, uint32_t sensor_data, uint8_t *output_data);
address_offset_t sensor_data_packet(uint8_t* output_data, uint16_t output_data_len_max, uint8_t battery, bool charging, int32_t tem);
#ifdef __cplusplus
}
#endif
#endif
`

export const cppFile = `#include <stdio.h>
#include <stdbool.h>
#include "sensor_data_packet.h"

#define LORA_DATA_MAX_LEN          200

typedef enum
{
    BATTERY_VOLTAGE_PARENT_ID = 0X00,
    CUSTOM_PARENT_ID0          = 0xFFF0,
    CUSTOM_PARENT_ID1          = 0xFFF1,
    CUSTOM_PARENT_ID2          = 0xFFF2,
    CUSTOM_PARENT_ID3          = 0xFFF3,
    CUSTOM_PARENT_ID4          = 0xFFF4,
    CUSTOM_PARENT_ID5          = 0xFFF5,
    CUSTOM_PARENT_ID6          = 0xFFF6,
    CUSTOM_PARENT_ID7          = 0xFFF7,
    CUSTOM_PARENT_ID8          = 0xFFF8,
    CUSTOM_PARENT_ID9          = 0xFFF9,
    CUSTOM_PARENT_IDA          = 0xFFFA,
    CUSTOM_PARENT_IDB          = 0xFFFB,
    CUSTOM_PARENT_IDC          = 0xFFFC,
    CUSTOM_PARENT_IDD          = 0xFFFD,
    CUSTOM_PARENT_IDE          = 0xFFFE,
    CUSTOM_PARENT_IDF          = 0xFFFF,
}parent_id_t;
typedef enum
{
    BATTERY_LEVEL_CHILD_ID          = 0x00,  //uint8_t 0-100 percent
    BATTERY_CHARGING_STATE_CHILD_ID = 0x01,  //uint8_t 0-1; 0:uncharge;1:charge
}battery_child_id_t;

#define ARRAY_TYPE_HEADER_LEN    (2)
#define EXCEPT_ARRAYS_HEADER_LEN (1)

typedef struct
{
    uint8_t data_header;
}sensor_data_t;

typedef enum
{
    USER_DATA_ARRAY_TYPE = 0,
    USER_DATA_DOUBLE_TYPE,
    USER_DATA_FLOAT_TYPE,
    USER_DATA_BOOL_TYPE,
    USER_DATA_INT8_T_TYPE,
    USER_DATA_UINT8_T_TYPE,
    USER_DATA_INT16_T_TYPE,
    USER_DATA_UINT16_T_TYPE,
    USER_DATA_INT32_T_TYPE,
    USER_DATA_UINT32_T_TYPE
}user_data_type_t;

address_offset_t array_type_data_packet(uint8_t sensor_data_id, uint8_t* sensor_data, uint8_t sensor_data_len, uint8_t *output_data)
{
    address_offset_t offset;
    if (sensor_data == NULL || output_data == NULL || (sensor_data_len > LORA_DATA_MAX_LEN))
    {
        return -1;
    }
    output_data[0] = ((sensor_data_id & 0x0F) << 4) + USER_DATA_ARRAY_TYPE;
    output_data[1] = sensor_data_len;
    for (int i = 0; i < sensor_data_len; i++)
    {
        output_data[ARRAY_TYPE_HEADER_LEN + i] = sensor_data[i];
    }
    offset = ARRAY_TYPE_HEADER_LEN + sensor_data_len;
    return offset;
}

address_offset_t double_type_data_packet(uint8_t sensor_data_id, double sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    if (output_data == NULL)
    {
        return -1;
    }
    double_type_convert_t double_data;
    double_data.double_raw_data = sensor_data;
    output_data[0] = ((sensor_data_id & 0x0F) << 4) + USER_DATA_DOUBLE_TYPE;
    for (int i = 0; i < BITS_64_TYPE_DATA_LEN; i++)
    {
        output_data[EXCEPT_ARRAYS_HEADER_LEN + i] = double_data.double_convert_data[i];
    }
    offset = EXCEPT_ARRAYS_HEADER_LEN + BITS_64_TYPE_DATA_LEN;
    return offset;
}


uint16_t get_sensor_parent_id(uint16_t address)
{
    return address;
}

uint8_t get_sensor_child_id(uint16_t address)
{
    return address;
}

address_offset_t parent_id_packet(parent_id_t id,uint8_t *begin_packet_address)
{
    bits_16_type_convert_t parent_id;
    parent_id.uint16_t_raw_data = get_sensor_parent_id(id);
    begin_packet_address[0] = parent_id.bits_16_convert_data[0];
    begin_packet_address[1] = parent_id.bits_16_convert_data[1];
    return 2;
}

address_offset_t bits_8_data_packet(uint8_t sensor_data_id, user_data_type_t data_type, bits_8_type_convert_t raw_data, uint8_t *output_data)
{
    address_offset_t offset;
    if (output_data == NULL)
    {
        return -1;
    }
    output_data[0] = ((sensor_data_id & 0x0F) << 4) + data_type;
    output_data[1] = raw_data.bits_8_convert_data[0];
    offset = EXCEPT_ARRAYS_HEADER_LEN + BITS_8_TYPE_DATA_LEN;
    return offset;
}

address_offset_t bool_type_data_packet(uint8_t sensor_data_id, bool sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_8_type_convert_t raw_data;
    raw_data.bool_raw_data = sensor_data;
    offset = bits_8_data_packet(sensor_data_id, USER_DATA_BOOL_TYPE, raw_data, output_data);
    return offset;
}
address_offset_t int8_t_type_data_packet(uint8_t sensor_data_id, int8_t sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_8_type_convert_t raw_data;
    raw_data.int8_t_raw_data = sensor_data;
    offset = bits_8_data_packet(sensor_data_id, USER_DATA_INT8_T_TYPE, raw_data, output_data);
    return offset;
}
address_offset_t uint8_t_type_data_packet(uint8_t sensor_data_id, uint8_t sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_8_type_convert_t raw_data;
    raw_data.uint8_t_raw_data = sensor_data;
    offset = bits_8_data_packet(sensor_data_id, USER_DATA_UINT8_T_TYPE, raw_data, output_data);
    return offset;
}

address_offset_t bits_16_data_packet(uint8_t sensor_data_id, user_data_type_t data_type, bits_16_type_convert_t raw_data, uint8_t *output_data)
{
    address_offset_t offset;
    if (output_data == NULL)
    {
        return -1;
    }
    output_data[0] = ((sensor_data_id & 0x0F) << 4) + data_type;
    for (int i = 0; i < BITS_16_TYPE_DATA_LEN; i++)
    {
        output_data[EXCEPT_ARRAYS_HEADER_LEN+i] = raw_data.bits_16_convert_data[i];
    }
    offset = EXCEPT_ARRAYS_HEADER_LEN +  BITS_16_TYPE_DATA_LEN;
    return offset;
}
address_offset_t int16_t_type_data_packet(uint8_t sensor_data_id, int16_t sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_16_type_convert_t raw_data;
    raw_data.int16_t_raw_data = sensor_data;
    offset = bits_16_data_packet(sensor_data_id, USER_DATA_INT16_T_TYPE, raw_data, output_data);
    return offset;
}
address_offset_t uint16_t_type_data_packet(uint8_t sensor_data_id, uint16_t sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_16_type_convert_t raw_data;
    raw_data.uint16_t_raw_data = sensor_data;
    offset = bits_16_data_packet(sensor_data_id, USER_DATA_UINT16_T_TYPE, raw_data, output_data);
    return offset;
}


address_offset_t bits_32_data_packet(uint8_t sensor_data_id, user_data_type_t data_type, bits_32_type_convert_t raw_data, uint8_t *output_data)
{
    address_offset_t offset;
    if (output_data == NULL)
    {
        return -1;
    }
    output_data[0] = ((sensor_data_id & 0x0F) << 4) + data_type;
    for (int i = 0; i < BITS_32_TYPE_DATA_LEN; i++)
    {
        output_data[EXCEPT_ARRAYS_HEADER_LEN + i] = raw_data.bits_32_convert_data[i];
    }
    offset = EXCEPT_ARRAYS_HEADER_LEN + BITS_32_TYPE_DATA_LEN;
    return offset;
}


address_offset_t float_type_data_packet(uint8_t sensor_data_id, float sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_32_type_convert_t raw_data;
    raw_data.float_raw_data = sensor_data;
    offset = bits_32_data_packet(sensor_data_id, USER_DATA_FLOAT_TYPE, raw_data, output_data);
    return offset;
}

address_offset_t int32_t_type_data_packet(uint8_t sensor_data_id, int32_t sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_32_type_convert_t raw_data;
    raw_data.int32_t_raw_data = sensor_data;
    offset = bits_32_data_packet(sensor_data_id, USER_DATA_INT32_T_TYPE, raw_data, output_data);
    return offset;
}

address_offset_t uint32_t_type_data_packet(uint8_t sensor_data_id, uint32_t sensor_data, uint8_t *output_data)
{
    address_offset_t offset;
    bits_32_type_convert_t raw_data;
    raw_data.uint32_t_raw_data = sensor_data;
    offset = bits_32_data_packet(sensor_data_id, USER_DATA_UINT32_T_TYPE, raw_data, output_data);
    return offset;
}
`