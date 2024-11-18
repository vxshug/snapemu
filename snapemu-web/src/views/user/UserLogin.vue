<template>
  <div>
    <v-form v-model="valid" @submit.prevent="login">
      <v-text-field
        v-model="user.username"
        :label="t('page.login.username')"
      ></v-text-field>
      <v-text-field
        v-model="user.password"
        type="password"
        :label="t('page.login.password')"
      ></v-text-field>
      <a
        class="text-caption text-decoration-none text-blue"
        href="#"
        rel="noopener noreferrer"
        target="_blank"
      >
        {{ t('page.login.forget') }}
      </a>

      <v-btn
        :block="true"
        type="submit"
        class="mb-8"
        color="blue"
        size="large"
        variant="tonal"
      >
        {{ t('page.login.login') }}
      </v-btn>
    </v-form>
    <v-card-text class="text-center">
      <RouterLink to="/user/signup">
        {{ t('page.login.signup') }} <v-icon icon="mdi-chevron-right"></v-icon>
      </RouterLink>
    </v-card-text>
  </div>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router';
import { useT } from '@/composables/i18n';
import { useSettingStore } from '@/stores/setting'
import { api } from '@/composables/api'
import { useSuccessMessage, useWarningMessage } from '@/composables/notify'

const setting = useSettingStore();
const router = useRouter();

interface User {
  username: string
  password: string
}
const t = useT()
const valid = ref()
const user = reactive<User>({
  username: '',
  password: '',
})
const login = async () => {
  if (!user.username) {
    useWarningMessage(t('page.login.empty_username'))
    return;
  }
  if (!user.password) {
    useWarningMessage(t('page.login.empty_password'))
    return;
  }
  try {
    const data = await api.postUserLogin(user.username, user.password);
    const cancel = useSuccessMessage(t("page.login.success"));
    setting.updateToken(data.access_token);
    setTimeout(() => {
      cancel();
      router.replace('/dashboard/');
    }, 1000);
  } catch (e: any) {
    useWarningMessage(e.message)
    console.log("error", e)
  }
}

const testLogin = async () => {
  try {
    await api.getUserInfo();
    await router.replace("/dashboard/");
  } catch (e) {
    console.log("catch", e);
  }
}

onMounted(async () => {
  await testLogin();
})

</script>
