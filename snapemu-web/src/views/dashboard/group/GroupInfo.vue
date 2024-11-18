<template>
  <v-container>
    <div class="header-container">
      <img :src="groupSvg" class="mr-4" alt=""/>
      <div>
        <h2>{{ groupData.name }}</h2>
      </div>
    </div>
    <v-divider :thickness="1">
    </v-divider>

    <v-row style="margin-top: 20px;">
      <v-col cols="6">
        <v-row class="mt-0">
          <v-col>
            <div class="container">
              <p class="label" style="margin-right: 100px;">{{t("page.dashboard.group.description")}}</p>
              <p class="value">{{ groupData.description }}</p>
            </div>
          </v-col>
        </v-row>
      </v-col>
    </v-row>

    <v-row
      :dense="true"
      class="mt-15">
      <v-col cols="8" class="mt-5">
        {{`${t("page.dashboard.group.device_count")}: ${groupData.device_count}`}}
      </v-col>

      <v-row>
        <v-col cols="8">
          <v-autocomplete
            :clearable="true"
            variant="solo-inverted" 
            placeholder="Search" 
            prepend-inner-icon="mdi-magnify"
            density="compact"
          ></v-autocomplete>
        </v-col>
        <v-col cols="4">
          <v-dialog width="800" v-if="!groupData.default_group">
            <template v-slot:activator="{ props }">
              <v-btn v-bind="props" :text="$t('page.dashboard.group.add_device')"> </v-btn>
            </template>

            <template v-slot:default="{ isActive }">
              <AddDevice @close="isActive.value = false" @update:device="updateGroupDevice" :group="groupId" :groupDevices="groupDevices"></AddDevice>
            </template>
          </v-dialog>
        </v-col>
      </v-row>

    </v-row>
    <v-divider :thickness="1" class="border-opacity-25">
    </v-divider>
    <v-table>
      <thead>
        <tr>
          <th class="text-left">
            {{t("page.dashboard.device.name")}}
          </th>
          <th class="text-left">
            {{t("page.dashboard.device.description")}}
          </th>
          <th class="text-left">
            {{t("page.dashboard.device.device_type")}}
          </th>
          <th class="text-left">
            {{t("page.dashboard.device.online_type")}}
          </th>
          <th class="text-left">
            {{t("page.dashboard.device.battery")}}
          </th>
          <th class="text-left">
            {{t("page.dashboard.group.remove_device")}}
          </th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in devices" :key="item.id">
          <td>{{ item.name }}</td>
          <td>{{ item.description }}</td>
          <td>{{ item.device_type }}</td>
          <td>{{ item.online ? $t("page.dashboard.device.online") : $t("page.dashboard.device.offline") }}</td>
          <td><DeviceBattery :battery="item.battery" /></td>
          <td><v-icon @click="removeDeviceHandler(item.id)" icon="mdi-minus" /></td>
        </tr>
      </tbody>
    </v-table>
  </v-container>
</template>

<script setup lang="ts">
import type { DeviceGroupResp } from '@/type/response'
import { computed, onMounted, ref } from 'vue'
import { useT } from '@/composables/i18n'
import { useRoute } from 'vue-router'
import DeviceBattery from '@/components/device/DeviceBattery.vue'
import { api } from '@/composables/api'
import groupSvg from '@/assets/icon/groups.svg'
import AddDevice from '@/views/dashboard/group/AddDevice.vue'

const t = useT();
const route = useRoute()
const groupId = route.params.id as string;

const groupData = ref<DeviceGroupResp>({
  default_group: false, description: '', device_count: 0, devices: [], id: '', name: '', offset: 0
});

const groupDevices = computed(() => groupData.value.devices.map(item => item.id));
const devices = ref(groupData.value.devices)
const removeDeviceHandler = async (device: string) => {
  if (groupData.value.default_group) {
    return;
  }
  await api.putGroup(groupId, {
    remove: [device]
  })
  await updateGroupDevice();
}

const updateGroupDevice = async () => {
  groupData.value = await api.getGroupInfo(groupId);
  devices.value = groupData.value.devices;
}

onMounted(async () => {
  await updateGroupDevice();
})



</script>

<style scoped lang="scss">
img {
  width: 50px;
  height: 50px;
}

.header-container {
  display: flex;
  align-items: center;
  margin-bottom: 10px;
}

.container {
  display: flex;
  flex-direction: row;
  align-items: flex-start;
  margin-bottom: 15px;
}

.label {
  color: rgb(153, 153, 158);
}
.value{
  font-size: 14px
}
</style>
