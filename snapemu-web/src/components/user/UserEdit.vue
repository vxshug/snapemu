<template>
  <v-card title="Dialog">
    <v-sheet>
      <v-form @submit.prevent="submit">
        <v-text-field
          v-model="password.value.value"
          :error-messages="password.errorMessage.value"
          :label="$t('page.login.password')"
        ></v-text-field>
        <v-text-field
          v-model="passwordConfirmation.value.value"
          :error-messages="passwordConfirmation.errorMessage.value"
          :label="$t('page.login.password_comfirm')"
        ></v-text-field>
        <v-btn
          :block="true"
          type="submit"
          class="mb-8"
          color="blue"
          size="large"
          variant="tonal"
        >
          {{ $t('page.login.signup') }}
        </v-btn>
        <v-btn @click="handlerClear">
          clear
        </v-btn>
      </v-form>
    </v-sheet>
  </v-card>
</template>

<script lang="ts" setup>
import { useField, useForm } from 'vee-validate';
import * as yup from "yup";
import { toTypedSchema } from '@vee-validate/yup';
import { api } from '@/composables/api'

const emits = defineEmits<{
  (e: "update:modelValue", value: boolean): void
}>();

const { handleSubmit, handleReset } = useForm({
  validationSchema: toTypedSchema(
    yup.object({
      password: yup.string().min(8).max(16).defined().default(""),
      passwordConfirmation: yup.string().required().oneOf([yup.ref("password")], "password not match"),
    }))
})

const handlerClear = () => {
  handleReset();
  emits("update:modelValue", false);
}


const password = useField<string>("password");
const passwordConfirmation = useField("passwordConfirmation");
const submit = handleSubmit(async () => {
  await api.putUserInfo({
    password: password.value.value,
    old_password: password.value.value,
  })
  setTimeout(() => {
  }, 1000);
})
</script>
