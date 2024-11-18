<template>
  <v-card
    class="mx-auto"
    width="344"
  >
    <v-card-text>
      <v-alert
        :type="alertType"
      >
        {{text}}
      </v-alert>
    </v-card-text>
    <v-card-text>
      <v-btn @click="verify">
        {{$t("page.user.verify")}}
      </v-btn>
    </v-card-text>
    <v-card-text>
      <RouterLink to="/user/login">
        <v-btn>
          {{$t("page.user.login")}}
        </v-btn>
      </RouterLink>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">

import { useRoute } from 'vue-router'
import { useT } from '@/composables/i18n'
import { ref } from 'vue'
import { api } from '@/composables/api'

const route = useRoute();
const t = useT();
const text = ref(t("page.user.wait"));
const alertType = ref("info")

const verify = async () => {
  try {
    text.value = await api.getActiveEmail(route.params.token as string);
    alertType.value = "success";
  } catch( e: any) {
    text.value = e.data;
    alertType.value = "error"
  }
}

</script>
