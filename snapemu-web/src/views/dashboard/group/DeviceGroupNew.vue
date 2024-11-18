<template>
  <v-dialog 
    v-model="dialogFlag" 
    class="p-2"
    :persistent="true"
    width="824"
  >
    <template v-slot:activator="{ props }">
      <v-btn variant="outlined" elevation="0" color="#D8D8D8" v-bind="props">
          <span class="text-black">{{ $t('page.dashboard.group.creat_group') }}</span>
      </v-btn>
    </template>
    <v-card class="pa-6" rounded="xl">
      <v-card-title>
        {{ $t('page.dashboard.group.creat_group') }}
      </v-card-title>
      <v-form @submit.prevent="submit">
        <v-text-field
          color="#0D5F5D" bg-color="#FFFFFF"
          single-line
          v-model="name.value.value"
          rounded="lg"
          variant="outlined"
          :error-messages="name.errorMessage.value"
          :label="$t('page.dashboard.group.group_name')"
        >
          <template v-slot:prepend>
            <div style="width: 100px"><span>{{$t('page.dashboard.group.group_name')}}</span></div>
          </template>
        </v-text-field>
        <v-text-field
          color="#0D5F5D" bg-color="#FFFFFF"
          single-line
          v-model="description.value.value"
          rounded="lg"
          variant="outlined"
          :error-messages="description.errorMessage.value"
          :label="$t('page.dashboard.group.description')"
        >
          <template v-slot:prepend>
            <div style="width: 100px"><span>{{$t('page.dashboard.group.description')}}</span></div>
          </template>
        </v-text-field>
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
            {{ $t('page.dashboard.group.create') }}
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
import { ref } from 'vue'
import { api } from '@/composables/api'

const dialogFlag = ref(false);

const { handleSubmit, handleReset } = useForm({
  validationSchema: toTypedSchema(
    object({
      name: string().required().min(4).max(16).default(""),
      description: string().required().default(""),
    }))
})

const name = useField("name");
const description = useField("description");

const cancelCreateHandler = () => {
  handleReset();
  dialogFlag.value = false;
}

const submit = handleSubmit(async (values) => {
  try {
    await api.postGroup({
      name: values.name,
      description: values.description
    });
    dialogFlag.value = false;
    
  } catch (e) {
    console.log(e);
  }
})

</script>
