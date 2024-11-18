<template>
  <v-card>
    <v-container>
      <v-row justify="center">
        <v-col cols="11">
          <form @submit.prevent="submit">
            <v-text-field
              v-model="name.value.value"
              variant="outlined"
              class="w-full"
              :error-messages="name.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.name')"
            ></v-text-field>

            <v-text-field
              v-model="description.value.value"
              variant="outlined"
              :error-messages="description.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.description')"
            ></v-text-field>
            <v-select
              v-model="deviceType.value.value"
              variant="outlined"
              :items="deviceTypeItem"
              :error-messages="deviceType.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.device_type')"
            ></v-select>
            <v-row :no-gutters="false" justify="space-between">
              <v-col>
                <v-select
                  v-model="group.value.value"
                  variant="outlined"
                  :multiple="true"
                  :items="deviceGroupsSelect"
                  :error-messages="group.errorMessage.value"
                  :label="$t('page.dashboard.device.addpage.group')"
                ></v-select>
              </v-col>
              <v-col>
                <v-select
                  v-model="region.value.value"
                  variant="outlined"
                  :items="loraRegion"
                  :error-messages="region.errorMessage.value"
                  :label="$t('page.dashboard.device.addpage.region')"
                ></v-select>
              </v-col>
            </v-row>

            <v-text-field
              v-if="isGateway"
              v-model="eui.value.value"
              variant="outlined"
              :error-messages="eui.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.eui')"
            ></v-text-field>
            <v-select
              v-if="isNode"
              v-model="joinType.value.value"
              variant="outlined"
              :items="loraJoinType"
              :error-messages="joinType.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.join_type')"
            ></v-select>

            <v-text-field
              v-if="isOTAA"
              v-model="appEui.value.value"
              variant="outlined"
              :error-messages="appEui.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.app_eui')"
            ></v-text-field>
            <v-text-field
              v-if="isOTAA"
              v-model="devEui.value.value"
              variant="outlined"
              :error-messages="devEui.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.dev_eui')"
            ></v-text-field>
            <v-text-field
              v-if="isOTAA"
              v-model="appKey.value.value"
              variant="outlined"
              :error-messages="appKey.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.app_key')"
            ></v-text-field>

            <v-text-field
              v-if="isABP"
              v-model="appSkey.value.value"
              variant="outlined"
              :error-messages="appSkey.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.app_skey')"
            ></v-text-field>
            <v-text-field
              v-if="isABP"
              v-model="nwkSkey.value.value"
              variant="outlined"
              :error-messages="nwkSkey.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.nwk_skey')"
            ></v-text-field>
            <v-text-field
              v-if="isABP"
              v-model="devAddr.value.value"
              variant="outlined"
              :error-messages="devAddr.errorMessage.value"
              :label="$t('page.dashboard.device.addpage.dev_addr')"
            ></v-text-field>

            <v-row justify="end">
              <v-btn @click="handleReset" class="mr-2">
                {{ t("page.dashboard.device.addpage.clear") }}
              </v-btn>
              <v-btn
                class="me-4"
                type="submit"
              >
                {{ t("page.dashboard.device.addpage.submit") }}
              </v-btn>
            </v-row>
          </form>
        </v-col>
      </v-row>
    </v-container>
  </v-card>
</template>


<script setup lang="ts">
import { useField, useForm } from 'vee-validate'
import { loraRegion, loraJoinType } from "@/type/lora"
import * as yup from 'yup';
import { toTypedSchema } from '@vee-validate/yup';
import { useT } from '@/composables/i18n'
import { computed, onMounted, ref } from 'vue'
import type { DeviceGroup } from '@/type/response'
import { isUndefined, omitBy } from 'lodash-es'
import { api } from '@/composables/api'
import { useSuccessMessage } from '@/composables/notify'

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
      region: yup.string().required(),
      device_type: yup.string().required().default("LoRaNode"),
      join_type: yup.string().default("OTAA").test("join type", "join type most require", (value, context) => {
        if (context.parent.device_type === "LoRaNode") {
          return !!value;
        }
        return true;
      }),
      eui: yup.string().length(16).uppercase().test("gateway eui","gateway eui most require and is hex value", (value) => {
        if (values.device_type === "LoRaGate") {
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
const appEui = useField<string>('join_parameter.app_eui')
const devEui = useField<string>('join_parameter.dev_eui')
const appSkey = useField<string>('join_parameter.app_skey')
const nwkSkey = useField<string>('join_parameter.nwk_skey')
const devAddr = useField<string>('join_parameter.dev_addr')
const deviceTypeItem = [ {
  title: "LoRaNode",
  value: "LoRaNode",
}, {
  title: "LoRaGateway",
  value: "LoRaGate",
} ];
const isGateway = computed(() => deviceType.value.value === "LoRaGate");
const isNode = computed(() => deviceType.value.value === "LoRaNode");
const isOTAA = computed(() => isNode.value && joinType.value.value === "OTAA");
const isABP = computed(() => isNode.value && joinType.value.value === "ABP");

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
  await api.postDevice(body);
  const cancel = useSuccessMessage('');
  emits("close")
  setTimeout(() => {
    cancel();
  }, 1000);
})

</script>
