<script setup lang="ts">
import type { GatewayDataEvent } from '@/type/event'
import DeviceAddr from '@/components/device/DeviceAddr.vue'
import TimeFormat from '@/components/widget/TimeFormat.vue'

const props = defineProps<{
  event: GatewayDataEvent
}>();

</script>

<template>
  <v-row>
    <v-col cols="1">
      <TimeFormat :time="props.event.time"/>
      <v-icon icon="mdi-arrow-up-bold-outline"></v-icon>
    </v-col>
    <v-col cols="1">
      <span>uplink</span>
    </v-col>
    <v-col class="flex" v-if="props.event.gateway_event.type === 'Status'">
      {{ JSON.stringify(props.event.gateway_event) }}
    </v-col>
    <v-col class="flex" v-if="props.event.gateway_event.type === 'Join'">
      {{ JSON.stringify(props.event.gateway_event) }}
    </v-col>
    <v-col class="flex" v-if="props.event.gateway_event.type === 'Data'">
      <DeviceAddr :dev-addr="props.event.gateway_event.dev_addr" ></DeviceAddr>
      <div>
        <span>Fcnt: </span>
        <span>{{ props.event.gateway_event.f_cnt}}</span>
      </div>
    </v-col>
  </v-row>
</template>

<style scoped lang="scss">

</style>