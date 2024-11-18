<template>
  <v-row justify="center">
    <div class="bg-line mt-5 rounded-[30px] overflow-hidden p-2" style="width: 1230px">
      <v-card
        v-if="device !== undefined"
        class="text-white"
        elevation="0"
        style="background-color: inherit"
      >
        <v-card-item>
          <v-card-title>
            <div class="flex justify-space-between pt-1 pb-1">
              <span class="text-5xl">{{device.name}}</span>
              <v-btn @click="toEditor" icon="mdi-pencil-outline"></v-btn>
            </div>
          </v-card-title>

          <v-card-subtitle>
            <div class="mt-1">
              <span class="mr-2">{{activeTime(device.active_time)}}</span>
              <DeviceBattery :battery="device.battery"/>
            </div>
          </v-card-subtitle>
        </v-card-item>

        <v-card-text class="pt-4 flex text-xl" style="height: 480px">
          <div v-if="isEditor && device.device_type==='LoRaNode'" class="w-full">
            <form @submit.prevent="submit">
              <v-row>
                <div class="w-2/5">
                  <v-select
                    v-model="productInfoId"
                    variant="outlined"
                    density="comfortable"
                    item-title="name"
                    item-value="id"
                    :items="productInfo"
                  >
                    <template v-slot:prepend>
                      <div style="width: 100px"><span class="text-red">*</span><span>product</span></div>
                    </template>
                  </v-select>
                </div>
                <v-text-field
                  v-model="name.value.value"
                  variant="outlined"
                  density="comfortable"
                  single-line
                  :error-messages="name.errorMessage.value"
                  :label="$t('page.dashboard.device.addpage.name')"
                >
                  <template v-slot:prepend>
                    <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.name')}}</span></div>
                  </template>
                </v-text-field>
              </v-row>

              <v-text-field
                v-model="description.value.value"
                variant="outlined"
                single-line
                density="comfortable"
                :error-messages="description.errorMessage.value"
                :label="$t('page.dashboard.device.addpage.description')"
              >
                <template v-slot:prepend>
                  <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.description')}}</span></div>
                </template>
              </v-text-field>
              <v-select
                v-model="region.value.value"
                variant="outlined"
                density="comfortable"
                :items="loraRegion"
                :error-messages="region.errorMessage.value"
              >
                <template v-slot:prepend>
                  <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.region')}}</span></div>
                </template>
              </v-select>
              <v-row>
                <v-col cols="1">
                  <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.join_type')}}</span></div>
                </v-col>
                <v-col cols="1">
                  <v-btn width="60" variant="outlined" :color="joinTypeHeightLight('ABP')"  class="!normal-case text-4xl" :ripple="false" @click="setABP">ABP</v-btn>
                </v-col>
                <v-col cols="1">
                  <v-btn width="60" variant="outlined" :color="joinTypeHeightLight('OTAA')" class="!normal-case" :ripple="false" @click="setOTAA">OTAA</v-btn>
                </v-col>
                <v-col>
                  <v-checkbox label="class_c" v-model="isClassC"></v-checkbox>
                </v-col>
              </v-row>
              <v-row>
                <v-text-field
                  single-line
                  v-model="appEui.value.value"
                  variant="outlined"
                  density="comfortable"
                  :error-messages="appEui.errorMessage.value"
                  :label="$t('page.dashboard.device.addpage.app_eui')"
                >
                  <template v-slot:prepend>
                    <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.app_eui')}}</span></div>
                  </template>
                </v-text-field>
                <v-text-field
                  single-line
                  v-model="appKey.value.value"
                  variant="outlined"
                  density="comfortable"
                  :error-messages="appKey.errorMessage.value"
                  :label="$t('page.dashboard.device.addpage.app_key')"
                >
                  <template v-slot:prepend>
                    <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.app_key')}}</span></div>
                  </template>
                </v-text-field>
              </v-row>
              <v-row>
                <v-text-field
                  single-line
                  v-model="appSkey.value.value"
                  variant="outlined"
                  density="comfortable"
                  :error-messages="appSkey.errorMessage.value"
                  :label="$t('page.dashboard.device.addpage.app_skey')"
                >
                  <template v-slot:prepend>
                    <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.app_skey')}}</span></div>
                  </template>
                </v-text-field>
                <v-text-field
                  single-line
                  v-model="nwkSkey.value.value"
                  variant="outlined"
                  density="comfortable"
                  :error-messages="nwkSkey.errorMessage.value"
                  :label="$t('page.dashboard.device.addpage.nwk_skey')"
                >
                  <template v-slot:prepend>
                    <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.nwk_skey')}}</span></div>
                  </template>
                </v-text-field>

              </v-row>
              <v-row justify="end">
                <v-btn
                  class="me-4 !normal-case text-white"
                  color="#0D5F5D"
                  rounded="xl"
                  width="110"
                  @click="isEditor=false"
                >
                  {{ t("page.dashboard.group.cancel") }}
                </v-btn>
                <v-btn
                  class="me-4 !normal-case text-white"
                  color="#0D5F5D"
                  type="submit"
                  rounded="xl"
                  width="110"
                >
                  {{ t("page.dashboard.device.addpage.submit") }}
                </v-btn>
              </v-row>
            </form>
          </div>

          <div v-else-if="isEditor && device.device_type === 'LoRaGate'" class="flex-1">
            <v-select
              v-model="productInfoId"
              variant="outlined"
              density="comfortable"
              item-title="name"
              item-value="id"
              :items="productInfo"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>product</span></div>
              </template>
            </v-select>
            <v-btn
              v-if="device.device_type === 'LoRaGate'"
              class="me-4 !normal-case"
              rounded="xl"
              width="110"
              @click="saveProductInfo"
            >
              {{ t("page.dashboard.device.addpage.submit") }}
            </v-btn>
          </div>
          <div v-else class="flex-1">
            <table class="text-left text-xl border-spacing-x-2.5 mt-2">
              <tbody class="device-info">
              <tr>
                <td> {{ $t("page.dashboard.device.description") }} </td>
                <td> {{ device.description }} </td>
              </tr>
              <tr>
                <td> {{ $t("page.dashboard.device.device_type") }} </td>
                <td> {{ device.device_type }} </td>
              </tr>
              <template v-if="device.device_type==='LoRaGate'">
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.eui") }} </td>
                  <td> {{ device.info.eui }} </td>
                </tr>
              </template>
              <template v-if="device.device_type==='Snap'">
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.app_key") }} </td>
                  <td> {{ device.info.key }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.eui") }} </td>
                  <td> {{ device.info.eui }} </td>
                </tr>
              </template>
              <template v-if="device.device_type==='LoRaNode'">
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.region") }} </td>
                  <td> {{ device.info.region }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.join_type") }} </td>
                  <td> {{ device.info.join_type }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.app_eui") }} </td>
                  <td> {{ device.info.app_eui }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.dev_eui") }} </td>
                  <td> {{ device.info.dev_eui }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.app_skey") }} </td>
                  <td> {{ device.info.app_skey }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.nwk_skey") }} </td>
                  <td> {{ device.info.nwk_skey }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.app_key") }} </td>
                  <td> {{ device.info.app_key }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.dev_addr") }} </td>
                  <td> {{ device.info.dev_addr }} </td>
                </tr>
                <tr>
                  <td> {{ $t("page.dashboard.device.settings.class_c") }} </td>
                  <td> {{ device.info.class_c }} </td>
                </tr>
              </template>

              </tbody>
            </table>
          </div>
          <div class="flex-1" v-if="!isEditor">
            <v-img :src="device.product_url" height="460"/>
          </div>
        </v-card-text>
      </v-card>
    </div>
  </v-row>
</template>

<script setup lang="ts">
import DeviceHub from "@/assets/icon/device-hub.png"
import { useRoute } from 'vue-router'
import { computed, onMounted, ref, watchEffect } from 'vue'
import { api } from '@/composables/api'
import DeviceInfo from '@/components/device/DeviceInfo.vue'
import { isUndefined, omitBy } from 'lodash-es'
import type { DeviceResp } from '@/type/response'
import { useActiveTime } from '@/composables/device_time'
import DeviceBattery from '@/components/device/DeviceBattery.vue'
import { useField, useForm } from 'vee-validate'
import { toTypedSchema } from '@vee-validate/yup'
import * as yup from 'yup'
import { useT } from '@/composables/i18n'
import { useSuccessMessage } from '@/composables/notify'
import { loraRegion } from '@/type/lora'

const isEditor = ref(false)
const productInfo = ref([])
const isClassC = ref(false)
const route = useRoute()
const deviceID = route.params.id as string;
const device = ref<DeviceResp>()
const activeTime = useActiveTime()
const isNode = computed(() => device.value?.device_type === "LoRaNode")

const toEditor = () => {
  isEditor.value = true;
}

const HexReg = /^[0-9a-fA-F]+$/;
const t = useT();

const { handleSubmit, handleReset } = useForm({
  validationSchema: toTypedSchema(
    yup.object({
      name: yup.string().required(),
      description: yup.string().required(),
      region: yup.string().required(),
      join_type: yup.string().default(""),
      app_eui: yup.string().length(16).uppercase().default(""),
      app_key: yup.string().length(32).uppercase().default(""),
      app_skey: yup.string().length(32).uppercase().default(""),
      nwk_skey: yup.string().length(32).uppercase().default(""),
    })
  )
})

const name = useField<string>('name')
const description = useField<string>('description')
const region = useField<string>('region')
const joinType = useField<string>('join_type')
const appKey = useField<string>('app_key')
const appEui = useField<string>('app_eui')
const appSkey = useField<string>('app_skey')
const nwkSkey = useField<string>('nwk_skey')

const productInfoId = ref()

onMounted(async () => {
  device.value = await api.getDeviceInfo(deviceID);
  if (device.value?.info.class_c) {
    isClassC.value = device.value?.info.class_c;
  }
  initForm();
  await updateProductInfo();
})

const initForm = () => {
  name.value.value = device.value?.name || ""
  description.value.value = device.value?.description || ""
  region.value.value = device.value?.info.region || ""
  joinType.value.value = device.value?.info.join_type || ""
  appEui.value.value = device.value?.info.app_eui || ""
  appKey.value.value = device.value?.info.app_key || ""
  appSkey.value.value = device.value?.info.app_skey || ""
  nwkSkey.value.value = device.value?.info.nwk_skey || ""
}
watchEffect(() => {
  if (appKey.value.value?.length > 32) {
    appKey.value.value = appKey.value.value.slice(0, 32)
  }
  if (appEui.value.value?.length > 16) {
    appEui.value.value = appEui.value.value.slice(0, 16)
  }

  if (appSkey.value.value?.length > 32) {
    appSkey.value.value = appSkey.value.value.slice(0, 32)
  }
  if (nwkSkey.value.value?.length > 32) {
    nwkSkey.value.value = nwkSkey.value.value.slice(0, 32)
  }
})

const joinTypeHeightLight = (device: string) => {
  if (joinType.value.value === device) {
    return "#FFFFFF"
  }
  return "#0D5F5D"
}
const setOTAA = () => {
  joinType.value.value = "OTAA";
}
const setABP = () => {
  joinType.value.value = "ABP";
}

const updateProductInfo = async () => {
  const info = await api.getProductInfo();
  productInfo.value = info;
  productInfoId.value = device.value.product_id;
}
const saveProductInfo = async () => {
  if (productInfoId.value === device.value.product_id) {
    return;
  }
  await api.putDeviceInfo(device.value.id, {
    product_id: productInfoId.value
  });
  device.value = await api.getDeviceInfo(deviceID);
  useSuccessMessage("updated");
}

const submit = handleSubmit(async (values) => {
  if (typeof device.value === "undefined") {
    return;
  }
  const info = {} as any;
  if (productInfoId.value !== device.value.product_id) {
    info.product_id = productInfoId.value;
  }
  if (values.name !== device.value?.name) {
    info.name = values.name;
  }
  if (values.description !== device.value?.description) {
    info.description = values.description;
  }
  if (values.region !== device.value?.info.region) {
    info.region = values.region;
  }
  if (isClassC.value !== device.value?.info.class_c) {
    info.class_c = isClassC.value;
  }
  if (values.join_type !== device.value?.info.join_type) {
    info.join_type = values.join_type;
  }
  if (values.app_eui !== device.value?.info.app_eui) {
    info.app_eui = values.app_eui;
  }
  if (values.app_key !== device.value?.info.app_key) {
    info.app_key = values.app_key;
  }
  if (values.app_skey !== device.value?.info.app_skey) {
    info.app_skey = values.app_skey;
  }
  if (values.nwk_skey !== device.value?.info.nwk_skey) {
    info.nwk_skey = values.nwk_skey;
  }
  await api.putDeviceInfo(device.value.id, info);
  device.value = await api.getDeviceInfo(deviceID);
  useSuccessMessage("updated");
  isEditor.value = false;
})
</script>

<style scoped>
.bg-line {
  background:linear-gradient(270deg, #209390 0%, #0D5F5D 100%, rgba(255, 255, 255, 0) 100%);
}
.device-info td {
  padding-top: 4px;
  padding-right: 20px;
}

</style>
