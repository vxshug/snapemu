<template>
  <v-sheet>
    <v-table>
      <tbody>
        <tr>
          <td> {{ $t("page.dashboard.device.name") }} </td>
          <td> {{ props.deviceInfo.name }} </td>
        </tr>
        <tr>
          <td> {{ $t("page.dashboard.device.description") }} </td>
          <td> {{ props.deviceInfo.description }} </td>
        </tr>
        <tr>
          <td> {{ $t("page.dashboard.device.device_type") }} </td>
          <td> {{ props.deviceInfo.device_type }} </td>
        </tr>
        <tr>
          <td> {{ $t("page.dashboard.device.settings.region") }} </td>
          <td> {{ props.deviceInfo.info.region }} </td>
        </tr>
        <tr v-if="isGate">
          <td> {{ $t("page.dashboard.device.settings.eui") }} </td>
          <td> {{ props.deviceInfo.info.eui }} </td>
        </tr>
        <template v-if="isNode">
          <tr>
            <td> {{ $t("page.dashboard.device.settings.join_type") }} </td>
            <td> {{ props.deviceInfo.info.join_type }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.app_eui") }} </td>
            <td> {{ props.deviceInfo.info.app_eui }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.dev_eui") }} </td>
            <td> {{ props.deviceInfo.info.dev_eui }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.app_skey") }} </td>
            <td> {{ props.deviceInfo.info.app_skey }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.nwk_skey") }} </td>
            <td> {{ props.deviceInfo.info.nwk_skey }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.app_key") }} </td>
            <td> {{ props.deviceInfo.info.app_key }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.dev_addr") }} </td>
            <td> {{ props.deviceInfo.info.dev_addr }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.class_b") }} </td>
            <td> {{ props.deviceInfo.info.class_b }} </td>
          </tr>
          <tr>
            <td> {{ $t("page.dashboard.device.settings.class_c") }} </td>
            <td> {{ props.deviceInfo.info.class_c }} </td>
          </tr>
        </template>
      </tbody>
    </v-table>
  </v-sheet>
</template>

<script setup lang="ts">

import { computed } from 'vue'

const props = defineProps<{
  deviceInfo: any;
}>();

const isNode = computed(() => props.deviceInfo.device_type === "LoRaNode")
const isGate = computed(() => props.deviceInfo.device_type === "LoRaGate")

</script>
