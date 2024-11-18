<script setup lang="ts">

import type { DownLinkDataEvent } from '@/type/event'
import TimeFormat from '@/components/widget/TimeFormat.vue'
import PayloadShow from '@/components/device/PayloadShow.vue'
import DeviceAddr from '@/components/device/DeviceAddr.vue'

const props = defineProps<{
  event: DownLinkDataEvent
}>();

</script>

<template>
  <v-row>
    <v-col cols="1">
      <TimeFormat :time="props.event.time"/>
      <v-icon icon="mdi-arrow-down-bold-outline"></v-icon>
    </v-col>
    <v-col cols="1">
      <span>downlink</span>
    </v-col>
    <v-col>
<!--      <HexShow :bytes="props.event"></HexShow>-->
    </v-col>

  </v-row>
</template>

<style scoped lang="scss">

</style>