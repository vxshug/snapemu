<template>
  <div>
    <v-form @submit.prevent="submit">
      <v-text-field
        v-model="username.value.value"
        :error-messages="username.errorMessage.value"
        :clearable="true"
        :label="t('page.login.username')"
      ></v-text-field>
      <v-text-field
        v-model="password.value.value"
        type="password"
        :error-messages="password.errorMessage.value"
        :label="t('page.login.password')"
      ></v-text-field>
      <v-text-field
        v-model="passwordConfirmation.value.value"
        type="password"
        :error-messages="passwordConfirmation.errorMessage.value"
        :label="t('page.login.password_confirm')"
      ></v-text-field>
      <v-text-field
        v-model="email.value.value"
        :error-messages="email.errorMessage.value"
        :label="t('page.login.email')"
      ></v-text-field>
      <v-btn
        :block="true"
        type="submit"
        class="mb-8"
        color="blue"
        size="large"
        variant="tonal"
      >
        {{ t('page.login.signup') }}
      </v-btn>
    </v-form>
    <v-card-text class="text-center">
      <RouterLink to="/user/login">
        {{ t('page.login.login') }} <v-icon icon="mdi-chevron-right"></v-icon>
      </RouterLink>
    </v-card-text>
  </div>
</template>

<script setup lang="ts">
import { useField, useForm } from 'vee-validate';
import * as yup from "yup";
import { toTypedSchema } from '@vee-validate/yup';
import { useRouter } from 'vue-router'
import { useSuccessMessage } from '@/composables/notify'
import { useT } from '@/composables/i18n'
import { api } from '@/composables/api'


const router = useRouter();


const t = useT()
const { handleSubmit } = useForm({
  validationSchema: toTypedSchema(
    yup.object({
      username: yup.string().required().default("").test("username", "username most only include a-z,A-Z,0-9, _ and -", (value) => {
        return /^[a-zA-Z0-9_-]{4,16}$/.test(value)
      }),
      password: yup.string().min(8).max(16).defined().default(""),
      passwordConfirmation: yup.string().required().oneOf([yup.ref("password")], "password not match"),
      email: yup.string().required().email()
    }))
})

const username = useField<string>("username");
const password = useField<string>("password");
const passwordConfirmation = useField<string>("passwordConfirmation");
const email = useField<string>("email");

const submit = handleSubmit(async () => {
  try {
    const msg = await api.postUserSignup(username.value.value, password.value.value, email.value.value);
    const cancel = useSuccessMessage(msg);
    setTimeout(() => {
      cancel()
      router.push("/user/login");
    }, 1000);
  } catch (e) {
    console.log(e);
  }
})
</script>
