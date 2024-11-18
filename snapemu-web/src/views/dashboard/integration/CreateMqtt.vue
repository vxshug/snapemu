<template>
  <v-dialog 
    v-model="dialogFlag" 
    class="p-2"
    :persistent="true"
    width="824"
  >
    <template v-slot:activator="{ props }">
      <v-btn variant="outlined" elevation="0" color="#D8D8D8" v-bind="props">
         <span class="text-black">{{ $t('page.dashboard.integration.create') }}</span>
      </v-btn>
    </template>
    <v-card class="pa-6" rounded="xl">
      <v-card-title>
        {{ $t('page.dashboard.integration.create') }}
      </v-card-title>
      <v-form @submit.prevent="submit">
        <v-text-field
          color="#0D5F5D" bg-color="#FFFFFF"
          single-line
          v-model="name.value.value"
          rounded="lg"
          variant="outlined"
          :error-messages="name.errorMessage.value"
          :label="$t('page.dashboard.integration.name')"
        >
          <template v-slot:prepend>
            <div style="width: 100px"><span>{{$t('page.dashboard.integration.name')}}</span></div>
          </template>
        </v-text-field>
        <div class="flex">
          <v-select
            v-model="device.value.value"
            variant="outlined"
            rounded="lg"
            :items="selectValue"
            item-title="name"
            item-value="id"
            :error-messages="device.errorMessage.value"
          >
            <template v-slot:prepend>
              <div style="width: 100px"><span>{{$t('page.dashboard.integration.device')}}</span></div>
            </template>
          </v-select>
          <v-checkbox :label="$t('page.dashboard.integration.group')" v-model="group"></v-checkbox>
        </div>
        <v-row justify="end">
          <v-btn
            @click="cancelCreateHandler"
            color="#8D8D8D"
            class="mr-4"
            rounded="xl"
            width="110"
          >
            {{ $t('page.dashboard.group.cancel') }}
          </v-btn>
          <v-btn
            class="me-4 !normal-case text-white"
            color="#0D5F5D"
            type="submit"
            rounded="xl"
            width="110"
          >
            {{ $t('page.dashboard.integration.create') }}
          </v-btn>
        </v-row>
      </v-form>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { useField, useForm } from 'vee-validate';
import { string, object } from "yup";
import { toTypedSchema } from '@vee-validate/yup';
import { computed, onMounted, ref, watch } from 'vue'
import type { DeviceAllResp, DeviceGroup } from '@/type/response'
import { api } from '@/composables/api'
import { loraRegion } from '@/type/lora'

const emits = defineEmits<{
  (e: "close"): void
}>();

const dialogFlag = ref(false);

const { handleSubmit, handleReset } = useForm({
  validationSchema: toTypedSchema(
    object({
      name: string().required().min(4).max(16).default(""),
      device: string().required().default(""),
    }))
})

const name = useField("name");
const device = useField("device");
const group = ref(false);

watch(group, () => {
  device.resetField();
})

const devices = ref<DeviceAllResp>({
  device_count: 0, devices: [], offset: 0
});
const groups = ref<DeviceGroup[]>([])

const updateDevices = async () => {
  devices.value = await api.getDevices();
}
const updateGroup= async () => {
  groups.value = await api.getGroups();
}

onMounted(async () => {
  await updateDevices();
  await updateGroup();
})

const selectValue = computed(() => group.value ? groups : devices.value.devices);

const cancelCreateHandler = () => {
  handleReset();
  dialogFlag.value = false;
}

const submit = handleSubmit(async (values) => {
  try {
    await api.postIntegration({
      group: group.value,
      name: values.name,
      device: parseInt(values.device)
    });
    dialogFlag.value = false;
    emits("close");
    
  } catch (e) {
    console.log(e);
  }
})

</script>

