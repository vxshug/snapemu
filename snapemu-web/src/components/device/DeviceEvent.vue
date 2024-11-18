<template>
  <div>
<!--    <template v-if="props.event.event.event === 'JoinAccept'">-->
<!--      <JoinAccept :event="props.event.event" />-->
<!--    </template>-->
<!--    <template v-if="props.event.event.event === 'JoinRequest'">-->
<!--      <JoinRequest :event="props.event.event" />-->
<!--    </template>-->
<!--    <template v-if="props.event.event.event === 'UplinkData'">-->
<!--      <UplinkData :event="props.event.event" />-->
<!--    </template>-->
<!--    <template v-if="props.event.event.event === 'DownLinkData'">-->
<!--      <DownLinkData :event="props.event.event" />-->
<!--    </template>-->
<!--    <template v-if="props.event.event.event === 'Gateway'">-->
<!--      <GatewayEvent :event="props.event.event" />-->
<!--    </template>-->
    <div>{{JSON.stringify(props.event.event)}}}</div>
  </div>
</template>

<script setup lang="ts">

import type { DeviceEventRes } from '@/type/event';
import JoinAccept from '@/components/event/JoinAccept.vue'
import JoinRequest from '@/components/event/JoinRequest.vue'
import UplinkData from '@/components/event/UplinkData.vue'
import DownLinkData from '@/components/event/DownLinkData.vue'
import GatewayEvent from '@/components/event/GatewayEvent.vue'

const props = defineProps<{
  event: DeviceEventRes
}>();

</script>
