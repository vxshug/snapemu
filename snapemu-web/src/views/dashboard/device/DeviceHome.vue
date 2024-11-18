<template>
  <div>
    <v-container>
      <v-row>
        <DeleteDialog :deleteName="deleteDevice.name" :cancel="closeDeleteDialog" :show="showDeleteDialog" :confirm="flushPage"/>
      </v-row>
      <v-row>
        <v-col cols="6">
        </v-col >
        <v-col cols="4">
          <div>
            <v-text-field
              v-model="search"
              :clearable="true"
              rounded="xl"
              variant="outlined"
              prepend-inner-icon="mdi-magnify"
              density="compact"
              label="Search"
              :single-line="true"
              hide-details
            ></v-text-field>
          </div>
        </v-col>
        <v-col cols="2">
          <div class="flex">
            <div class="flex align-center" @click="changeLanguage">
              <EarthSvg></EarthSvg>
            </div>
            <v-btn icon="mdi-plus" elevation="0" @click="createDevice"></v-btn>
          </div>
        </v-col>
      </v-row>
    </v-container>
    <v-card color="#F6F6FA" width="1230px" class="mx-auto" elevation="6" rounded="xl">
      <v-container class="bg-white">
        <v-row>
          <v-col cols="12">
            <div class="flex justify-space-between">
              <div>
                <v-slide-group
                  show-arrows
                  v-model="groupValue"
                >
                  <v-slide-group-item
                    v-for="(item, index) in deviceGroups"
                    :key="item.id"
                    :value="index"
                    v-slot="{ isSelected, toggle }"
                  >
                    <v-btn
                      class="mr-1 !text-2xl !normal-case "
                      :class="{ '!border-b-blue-400': isSelected, '!border-b-4': isSelected }"
                      variant="text"
                      @click="click(toggle, index)"
                    >
                      {{ item.name }}
                    </v-btn>
                  </v-slide-group-item>
                </v-slide-group>
              </div>
              <div><span>{{t('page.dashboard.device.device_count')}}: </span><span>{{deviceCount}}</span></div>
            </div>
          </v-col>
        </v-row>
        <v-row justify="space-around">
          <template v-for="item in deviceInfo" :key="item.id">
            <v-card class="rounded-xl !border-2 hover:!border-sky-400" @click="goDeviceInfo(item.id)" elevation="0">
              <DeviceSingleCard v-if="deviceInfo.length === 1" :device="item" color="#076360" @delete="deviceHandler" @top="deviceTopHandler">
              </DeviceSingleCard>
              <DeviceDoubleCard v-if="deviceInfo.length === 2" :device="item" color="#076360" @delete="deviceHandler" @top="deviceTopHandler">
              </DeviceDoubleCard>
              <DeviceCard v-if="deviceInfo.length === 3" :device="item" color="#076360" @delete="deviceHandler" @top="deviceTopHandler">
              </DeviceCard>
            </v-card>
          </template>
        </v-row>
        <div style="height: 120px;" class="flex justify-space-between pt-10">
          <div></div>
          <div class="flex justify-space-between">
            <span v-for="i in countDot" :key="i" @click="setCurrentIndex(i-1)" class="rounded-full mr-2 bg-black" style="display: block; height: 8px; width: 8px"></span>
          </div>
          <div>
            <v-btn class="mr-3 scale-90" icon="mdi-chevron-left" elevation="0" color="#D8D8D8" @click="setCurrentIndex(currentIndex-1)"></v-btn>
            <v-btn class="scale-90" icon="mdi-chevron-right" elevation="0" color="#D8D8D8" @click="setCurrentIndex(currentIndex+1)"></v-btn>
          </div>
        </div>
      </v-container>
    </v-card>
  </div>
</template>
  
<script setup lang="ts">
import type { DeviceAllResp, DeviceGroup } from '@/type/response'
import { computed, onMounted, ref } from 'vue'
import { useI18n, useT } from '@/composables/i18n'
import { useRouter } from 'vue-router'
import DeleteDialog from '@/components/interaction/DeleteDialog.vue'
import DeviceCard from '@/views/dashboard/device/DeviceCard.vue'
import { api } from '@/composables/api'
import EarthSvg from '@/components/widget/EarthSvg.vue'
import DeviceSingleCard from '@/views/dashboard/device/details/DeviceSingleCard.vue'
import DeviceDoubleCard from '@/views/dashboard/device/details/DeviceDoubleCard.vue'

const search = ref('');

const t = useT()
const i18n = useI18n()
const router = useRouter();
const devices = ref<DeviceAllResp>({ device_count: 0, devices: [], offset: 0 });
const deviceGroups = ref<DeviceGroup[]>([]);
const groupCurrent = ref('');
const groupValue = ref(0);
const showDeleteDialog = ref(false);
const deleteDevice = ref({id: '', name: ''});

const createDevice = () => {
  router.push('/dashboard/add/device');
};

const groupUpdate = async () => {
  const groups = await api.getGroups();
  groups.sort((p, c) => (+c.default_group) - (+p.default_group) );
  groupCurrent.value = groups[0].id;
  deviceGroups.value = groups;
}

const deviceUpdate = async () => {
  devices.value = await api.getDevices();
}

onMounted(async () => {
  await groupUpdate();
  await deviceUpdate();
})
const closeDeleteDialog = () => { showDeleteDialog.value = false };

const click = async (toggle: () => void, index: number) => {
  let groupId = deviceGroups.value[index].id;
  groupCurrent.value = groupId;
  devices.value = await api.getGroupInfo(groupId);
  toggle();
}

const goDeviceInfo = (id: string) => {
  router.push(`/dashboard/device/${id}/index`)
}


const currentIndex = ref(0)

const deviceCount = computed(() => devices.value.device_count);
const deviceInfoFilter = computed(() =>
  devices.value.devices.map(item => {
    return {
      id: item.id,
      name: item.name,
      data: item.data,
      battery: item.battery,
      product_id: item.product_id,
      product_url: item.product_url,
      description: item.description,
      active_time: item.active_time
    }
  }).filter(item => {
    if (search.value.length < 1) {
      return true;
    } else {
      return item.name.includes(search.value);
    }
  })
);

const changeLanguage = () => {
  i18n.locale.value = i18n.locale.value === 'zh' ? 'en' : 'zh'
}
const deviceInfo = computed(() =>
  deviceInfoFilter.value.slice(currentIndex.value * 3, currentIndex.value *3 + 3)
);

const countDot = computed(() => Math.ceil(deviceInfoFilter.value.length / 3))

const setCurrentIndex = (index: number) => {
  window.console.log(index)
  if (index < 0 || index >= countDot.value) {
    return
  }
  currentIndex.value = index
}

const flushPage = async () => {
  await api.deleteDevice(deleteDevice.value.id);
  devices.value = await api.getDevices();
  deviceGroups.value = await api.getGroups();
  closeDeleteDialog();
}

const deviceHandler = (device: {id: string, name: string}) => {
  deleteDevice.value = device;
  showDeleteDialog.value = true;
}

const deviceTopHandler = async (id: string) => {
  await api.putDeviceTop(id, groupCurrent.value);
  await deviceUpdate();
}
</script>
