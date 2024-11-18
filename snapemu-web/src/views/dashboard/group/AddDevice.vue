<script setup lang="ts">

import { onMounted, ref } from 'vue'
import type { ShowDevice } from '@/type/response'
import { api } from '@/composables/api'

const props = defineProps<{
  group: string,
  groupDevices: string[]
}>();

const emits = defineEmits<{
  (e: 'close'): void
  (e: 'update:device'): void
}>();

const closeDialog = () => {
  emits('close');
}

const updateGroupDevice = () => {
  emits('update:device');
}

const userDevices = ref<ShowDevice>({ device_count: 0, devices: [], offset: 0 });

const deviceShow = ref<{
  id: string,
  name: string,
  include: boolean
}[]>([]);

const updateDevice = async () => {
  userDevices.value = await api.getShowDevice();
  deviceShow.value = userDevices.value.devices.map(item => ({
    id: item.id,
    name: item.name,
    include: props.groupDevices.includes(item.id)
  }))
}

const submitHandler = async () => {
  const remove: string[] = [];
  const devices: string[] = [];
  deviceShow.value.forEach(item => {
    if (item.include) {
      if (!props.groupDevices.includes(item.id)) {
        devices.push(item.id);
      }
    } else {
      if (props.groupDevices.includes(item.id)) {
        remove.push(item.id);
      }
    }
  })
  await api.putGroup(props.group, {
    devices,
    remove
  })
  updateGroupDevice();
  closeDialog();
}
onMounted(async () => {
  await updateDevice();
})

</script>

<template>
  <v-card >
    <v-card-text>
      <v-table height="300px">
        <thead>
        <tr>
          <th class="text-left">
            {{ $t("page.dashboard.device.id") }}
          </th>
          <th class="text-left">
            {{ $t("page.dashboard.device.name") }}
          </th>
          <th class="text-left">
            {{ $t("page.dashboard.group.include_device") }}
          </th>
        </tr>
        </thead>
        <tbody>
        <tr
          v-for="item in deviceShow"
          :key="item.id"
        >
          <td>{{ item.id }}</td>
          <td>{{ item.name }}</td>
          <td> <v-checkbox v-model="item.include"></v-checkbox> </td>
        </tr>
        </tbody>
      </v-table>
    </v-card-text>

    <v-card-actions>
      <v-btn
        text="Close"
        @click="closeDialog"
      ></v-btn>
      <v-spacer></v-spacer>
      <v-btn
        text="Save"
        @click="submitHandler"
      ></v-btn>
    </v-card-actions>
  </v-card>
</template>

<style scoped lang="scss">

</style>