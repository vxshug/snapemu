<template>
  <HeadBack />
  <v-card class="ml-auto mr-auto" width="1230" height="820" rounded="xl">
    <v-card-title>
      <span class="text-4xl text-black">
        add device
      </span>
      <div class="border-b-sm"></div>
    </v-card-title>
    <div class="ml-auto mr-auto" style="width: 600px">
      <v-row justify="center">
        <v-col >
          <form @submit.prevent="submit">
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              v-model="name.value.value"
              variant="outlined"
              single-line
              class="w-full"
              :error-messages="name.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.name')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.name')}}</span></div>
              </template>
            </v-text-field>

            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              v-model="description.value.value"
              variant="outlined"
              single-line
              :error-messages="description.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.description')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.description')}}</span></div>
              </template>
            </v-text-field>
            <div class="flex">
              <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.device_type')}}</span></div>
              <div style="width: 15px; height: 50px"></div>
              <div class="flex flex-1 justify-space-between">
                <v-btn width="120" variant="outlined" :color="deviceTypeHeightLight('LoRaGate')"  class="!normal-case text-4xl" :ripple="false" @click="setLoRaGateway">LoRaGateway</v-btn>
                <v-btn width="120" variant="outlined" :color="deviceTypeHeightLight('LoRaNode')" class="!normal-case" :ripple="false" @click="setLoRaNode">LoRaNode</v-btn>
                <v-btn width="120" variant="outlined" :color="deviceTypeHeightLight('Snap')" class="!normal-case" :ripple="false" @click="setSnap">Snap</v-btn>
              </div>
            </div>
            <v-select
              v-model="group.value.value"
              variant="outlined"
              :multiple="true"
              :items="deviceGroupsSelect"
              :error-messages="group.errorMessage.value"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.group')}}</span></div>
              </template>
            </v-select>
            <v-select
              v-if="!isSnap"
              v-model="region.value.value"
              variant="outlined"
              :items="loraRegion"
              :error-messages="region.errorMessage.value"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.region')}}</span></div>
              </template>
            </v-select>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isGateway"
              v-model="eui.value.value"
              variant="outlined"
              :error-messages="eui.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.eui')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.eui')}}</span></div>
              </template>
            </v-text-field>
            <div class="flex" v-if="isNode">
              <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.join_type')}}</span></div>
              <div style="width: 15px; height: 50px"></div>
              <div class="flex flex-1 justify-space-between">
                <v-btn width="120" variant="outlined" :color="joinTypeHeightLight('ABP')"  class="!normal-case text-4xl" :ripple="false" @click="setABP">ABP</v-btn>
                <v-btn width="120" variant="outlined" :color="joinTypeHeightLight('OTAA')" class="!normal-case" :ripple="false" @click="setOTAA">OTAA</v-btn>
              </div>
            </div>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isOTAA"
              v-model="appEui.value.value"
              variant="outlined"
              :error-messages="appEui.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.app_eui')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.app_eui')}}</span></div>
              </template>
            </v-text-field>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isNode"
              v-model="devEui.value.value"
              variant="outlined"
              :error-messages="devEui.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.dev_eui')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.dev_eui')}}</span></div>
              </template>
            </v-text-field>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isOTAA"
              v-model="appKey.value.value"
              variant="outlined"
              :error-messages="appKey.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.app_key')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.app_key')}}</span></div>
              </template>
            </v-text-field>

            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isABP"
              v-model="appSkey.value.value"
              variant="outlined"
              :error-messages="appSkey.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.app_skey')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.app_skey')}}</span></div>
              </template>
            </v-text-field>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isABP"
              v-model="nwkSkey.value.value"
              variant="outlined"
              :error-messages="nwkSkey.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.nwk_skey')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.nwk_skey')}}</span></div>
              </template>
            </v-text-field>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isABP"
              v-model="devAddr.value.value"
              variant="outlined"
              :error-messages="devAddr.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.dev_addr')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.dev_addr')}}</span></div>
              </template>
            </v-text-field>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isSnap"
              v-model="eui.value.value"
              variant="outlined"
              :error-messages="eui.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.snap_eui')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.snap_eui')}}</span></div>
              </template>
            </v-text-field>
            <v-text-field
              color="#0D5F5D" bg-color="#FFFFFF"
              single-line
              v-if="isSnap"
              v-model="snapKey.value.value"
              variant="outlined"
              :error-messages="snapKey.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.snap_key')"
            >
              <template v-slot:prepend>
                <div style="width: 100px"><span class="text-red">*</span><span>{{$t('page.dashboard.device.addpage.snap_key')}}</span></div>
              </template>
            </v-text-field>
            <v-row justify="end">
              <v-btn @click="handleReset" class="mr-2 !normal-case text-white" color="#0D5F5D" rounded="xl" width="110">
                {{ t("page.dashboard.device.addpage.clear") }}
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
        </v-col>
      </v-row>
    </div>
  </v-card>
</template>


<script setup lang="ts">
import { useField, useForm } from 'vee-validate'
import { loraRegion, loraJoinType } from "@/type/lora"
import * as yup from 'yup';
import { toTypedSchema } from '@vee-validate/yup';
import { useT } from '@/composables/i18n'
import { computed, onMounted, ref, watch, watchEffect } from 'vue'
import type { DeviceGroup } from '@/type/response'
import { isUndefined, omitBy } from 'lodash-es'
import { api } from '@/composables/api'
import { useSuccessMessage, useWarningMessage } from '@/composables/notify'
import TextInput from '@/components/widget/TextInput.vue'
import BackSvg from '@/components/widget/BackSvg.vue'
import router from '@/router'
import HeadBack from '@/components/widget/HeadBack.vue'

const HexReg = /^[0-9a-fA-F]+$/;


const t = useT();
const emits = defineEmits(["close"]);
const HexTest = (value?: string) => {
  if(value) {
    return HexReg.test(value)
  } else {
    return false;
  }
};

const { handleSubmit, handleReset, values } = useForm({
  validationSchema: toTypedSchema(
    yup.object({
      name: yup.string().required(),
      description: yup.string().required(),
      group: yup.array().required().default([]),
      region: yup.string().test("region", "region is required", (value) => {
        if (values.device_type === "LoRaGate" || values.device_type === "LoRaNode") {
          return !!value
        }
        return true;
      }),
      device_type: yup.string().required().default("LoRaNode"),
      join_type: yup.string().default("OTAA").test("join type", "join type most require", (value, context) => {
        if (context.parent.device_type === "LoRaNode") {
          return !!value;
        }
        return true;
      }),
      eui: yup.string().length(16).uppercase().test("gateway eui","eui most require and is hex value", (value) => {
        if (values.device_type === "LoRaGate" || values.device_type === "Snap") {
          console.log(value)
          return HexTest(value)
        }
        return true;
      }),
      join_parameter: yup.object({
        app_eui: yup.string().length(16).uppercase().test("app eui", "app eui most require and is hex value", (value) => {
          if (values.device_type === "LoRaNode" && values.join_type === "OTAA") {
            return HexTest(value)
          }
          return true;
        }),
        dev_eui: yup.string().length(16).uppercase().test("dev eui","dev eui most require and is hex value", (value) => {
          if (values.device_type === "LoRaNode" && values.join_type === "OTAA") {
            return HexTest(value)
          }
          return true;
        }),
        app_key: yup.string().length(32).uppercase().test("app key", "app key most require and is hex value", (value) => {
          if (values.device_type === "LoRaNode" && values.join_type === "OTAA") {
            return HexTest(value)
          }
          return true;
        }),
        key: yup.string().length(32).uppercase().test("snap key", "snap key most require and is hex value", (value) => {
          if (values.device_type === "Snap") {
            return HexTest(value)
          }
          return true;
        }),
        app_skey: yup.string().length(32).uppercase().test("app skey", "app skey most require and is hex value",  (value) => {
          if (values.device_type === "LoRaNode" && values.join_type === "ABP") {
            return HexTest(value)
          }
          return true;
        }),
        nwk_skey: yup.string().length(32).uppercase().test("nwk skey", "nwk skey most require and is hex value",  (value) => {
          if (values.device_type === "LoRaNode" && values.join_type === "ABP") {
            return HexTest(value)
          }
          return true;
        }),
        dev_addr: yup.string().length(8).uppercase().test("dev addr", "dev addr most require and is hex value",  (value) => {
          if (values.device_type === "LoRaNode" && values.join_type === "ABP") {
            return HexTest(value)
          }
          return true;
        }),
      })
    })
  )
})
const name = useField<string>('name')
const description = useField<string>('description')
const group = useField<string>('group')
const deviceType = useField<string>('device_type')
const region = useField<string>('region')
const eui = useField<string>('eui')
const joinType = useField<string>('join_type')
const appKey = useField<string>('join_parameter.app_key')
const snapKey = useField<string>('join_parameter.key')
const appEui = useField<string>('join_parameter.app_eui')
const devEui = useField<string>('join_parameter.dev_eui')
const appSkey = useField<string>('join_parameter.app_skey')
const nwkSkey = useField<string>('join_parameter.nwk_skey')
const devAddr = useField<string>('join_parameter.dev_addr')

watchEffect(() => {
  if (eui.value.value?.length > 16) {
    eui.value.value = eui.value.value.slice(0, 16)
  }
  if (appKey.value.value?.length > 32) {
    appKey.value.value = appKey.value.value.slice(0, 32)
  }
  if (snapKey.value.value?.length > 32) {
    snapKey.value.value = snapKey.value.value.slice(0, 32)
  }
  if (appEui.value.value?.length > 16) {
    appEui.value.value = appEui.value.value.slice(0, 16)
  }
  if (devEui.value.value?.length > 16) {
    devEui.value.value = devEui.value.value.slice(0, 16)
  }
  if (appSkey.value.value?.length > 32) {
    appSkey.value.value = appSkey.value.value.slice(0, 32)
  }
  if (nwkSkey.value.value?.length > 32) {
    nwkSkey.value.value = nwkSkey.value.value.slice(0, 32)
  }
  if (devAddr.value.value?.length > 8) {
    devAddr.value.value = devAddr.value.value.slice(0, 8)
  }
})
const deviceTypeItem = [ {
  title: "LoRaNode",
  value: "LoRaNode",
}, {
  title: "LoRaGateway",
  value: "LoRaGate",
} ];
const isGateway = computed(() => deviceType.value.value === "LoRaGate");
const isNode = computed(() => deviceType.value.value === "LoRaNode");
const isSnap = computed(() => deviceType.value.value === "Snap");
const isOTAA = computed(() => isNode.value && joinType.value.value === "OTAA");
const isABP = computed(() => isNode.value && joinType.value.value === "ABP");

const setLoRaGateway = () => {
  deviceType.value.value = "LoRaGate";
}
const setLoRaNode = () => {
  deviceType.value.value = "LoRaNode";
}
const setSnap = () => {
  deviceType.value.value = "Snap";
}
const setOTAA = () => {
  joinType.value.value = "OTAA";
}
const setABP = () => {
  joinType.value.value = "ABP";
}

const deviceTypeHeightLight = (device: string) => {
  if (deviceType.value.value === device) {
    return "#0D5F5D"
  }
}

const joinTypeHeightLight = (device: string) => {
  if (joinType.value.value === device) {
    return "#0D5F5D"
  }
}

const deviceGroups = ref<DeviceGroup[]>([])
const deviceGroupsSelect = computed(() => {
  if (isUndefined(deviceGroups.value)) {
    return []
  } else {
    return deviceGroups.value.map(item => {
      return {
        title: item.name,
        value: item.id
      }
    })
  }
})

onMounted(async () => {
  deviceGroups.value = await api.getGroups();
})

const submit = handleSubmit(async (values) => {
  const body = omitBy(values, isUndefined)
  try {
    await api.postDevice(body);
  } catch (error) {
    useWarningMessage(error)
    return;
  }
  useSuccessMessage("create success");
  emits("close")

})

</script>
