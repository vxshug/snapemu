<template>
  <v-card class="bg-line text-white" width="1174" height="580">
    <v-card-title>
      <div class="flex justify-space-between">
        <p class="text-5xl ml-1 align-center">{{props.device.name}}</p>
        <v-menu>
          <template v-slot:activator="{ props }">
            <v-btn
              @click="props.onClick"
              variant="text"
              icon="mdi-dots-horizontal"
            >
            </v-btn>
          </template>
          <v-list>
            <v-list-item>
              <RouterLink :to="`/dashboard/device/${props.device.id}/info`">{{ $t("page.dashboard.device.device_info") }}</RouterLink>
            </v-list-item>
            <v-list-item @click="emits('top', props.device.id)">
              {{ $t("page.dashboard.device.top") }}
            </v-list-item>
            <v-list-item @click="emits('delete', props.device)">
              {{ $t("page.dashboard.device.delete") }}
            </v-list-item>
          </v-list>
        </v-menu>
      </div>
    </v-card-title>
    <div style="height: 20px" ></div>
    <div class="pa-5 flex justify-space-between h-[448px]">
      <div class="flex flex-column justify-space-between w-[480px]">
        <ActiveTimeView class="text-3xl" :time="props.device.active_time" />
        <div class="flex justify-space-around flex-column" style="height: 200px">
          <template v-for="(item, index) in deviceData" :key="item.data_id">
            <DataCard
              :name="item.name"
              :value="item.data.data"
              :unit="item.unit"
              :only="props.device.data.length === 1"
              :color="indexToBg(index)"
            />
          </template>
        </div>
      </div>
      <div class="h-[540px]">
        <div class="w-[300px]"></div>
        <v-img :src="props.device.product_url" height="200"/>
      </div>
      <div class="flex flex-column justify-end pr-2 h-full">
        <Battery
          v-if="!isUndefined(props.device.battery)"
          :battery="props.device.battery"
        />
      </div>
    </div>

  </v-card>
</template>
<script setup lang="ts">
import type { DeviceOneData } from "@/type/response"
import { indexToBg } from "@/utils/define";
import { isUndefined } from 'lodash-es'
import ActiveTimeView from '@/components/widget/ActiveTimeView.vue'
import Battery from '@/components/widget/Battery.vue'
import DataCard from '@/views/dashboard/device/DataCard.vue'
import DeviceHub from "@/assets/icon/device-hub.png"
import { computed, ref} from 'vue'

const deviceData = computed(() => {
  if (typeof props.device.data === "undefined") {
    return []
  } else {
    return props.device.data.slice(0,2)
  }
})

const emits = defineEmits<{
  (e: "delete", device: { id: string, name: string }): void
  (e: "top", id: string): void
}>()

const props = defineProps<{
  color: string,
  device: {
    id: string,
    name: string,
    active_time?: number,
    battery?: number,
    charge?: boolean,
    data: DeviceOneData[],
  }
}>()

</script>

<style lang="scss" scoped>
.bg-line {
  background: linear-gradient(72deg, #074443 9%, #2B9C98 84%);;
}
</style>/
