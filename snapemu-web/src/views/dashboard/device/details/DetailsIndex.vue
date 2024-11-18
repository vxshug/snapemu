<template>
  <v-card class="mr-auto ml-auto" width="1230" elevation="6">
    <v-card-title >
      <span class="text-4xl text-black">{{deviceInfo?.name}}</span>
      <div class="border-b-sm"></div>
    </v-card-title>
    <template v-for="(item, index) in deviceLastData?.data"
              :key="item.data_id">
      <DataView
        :data="item"
        :titleBg="indexToBg(index)"
      />
    </template>
    <div class="flex justify-space-between">
      <div class="flex">
        <div class="ml-6">
          <span>
            {{ $t("page.dashboard.device.description")}}:
          </span>
          <span>
            {{deviceInfo?.description}}
          </span>
        </div>
        <div class="ml-8">
          <span>
            {{ $t("page.dashboard.device.device_type")}}:
          </span>
          <span>
            {{deviceInfo?.device_type}}
          </span>
        </div>
      </div>
      <div>
        <v-btn class="mr-3 scale-90" icon="mdi-chevron-left" elevation="0" color="#D8D8D8"></v-btn>
        <v-btn class="scale-90" icon="mdi-chevron-right" elevation="0" color="#D8D8D8"></v-btn>
      </div>
    </div>
  </v-card>
</template>

<script setup lang="ts">
import type { DeviceResp, DataResp} from "@/type/response";
import { indexToBg } from "@/utils/define";
import { useT } from '@/composables/i18n'
import { useRoute } from 'vue-router'
import { onMounted, ref } from 'vue'
import ActiveTime from '@/components/widget/ActiveTime.vue'
import DataView from '@/components/widget/DataView.vue'
import { api } from '@/composables/api'
const t = useT();
const route = useRoute()
const deviceID = route.params.id as string;

const deviceInfo = ref<DeviceResp>();
const deviceLastData  = ref<DataResp>()

const updateDeviceInfo = async () => {
  deviceInfo.value = await api.getDeviceInfo(deviceID);
}

const updateDate = async () => {
  deviceLastData.value = await api.getDeviceData(deviceID);
}

onMounted(async () => {
  await updateDate();
  await updateDeviceInfo();
})

</script>

<style scoped>
img {
  width: 60px;
  height: 60px;

}
</style>

