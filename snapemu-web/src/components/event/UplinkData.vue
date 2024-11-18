<script setup lang="ts">

import type { UplinkDataEvent } from '@/type/event'
import PayloadShow from '@/components/device/PayloadShow.vue'
import TimeFormat from '@/components/widget/TimeFormat.vue'
import DeviceAddr from '@/components/device/DeviceAddr.vue'

const props = defineProps<{
  event: UplinkDataEvent
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
    <v-col class="flex">
      <DeviceAddr :dev-addr="props.event.dev_addr" ></DeviceAddr>
      <PayloadShow :bytes="props.event.decoded_payload"></PayloadShow>
    </v-col>
  </v-row>
</template>

<style scoped lang="scss">

</style>