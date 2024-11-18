<template>
  <v-card title="Dialog">
    <v-sheet>
      <v-form @submit.prevent="submit">
        <v-file-input
          accept="image/*"
          v-model="picture"
          label="File input"
        ></v-file-input>
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

      </v-form>
    </v-sheet>
  </v-card>
</template>

<script lang="ts" setup>
import { useT } from '@/composables/i18n'
import { ref } from 'vue'
import { api } from '@/composables/api'
import { useSuccessMessage } from '@/composables/notify'

const emits = defineEmits<{
  (e: "editReset"): void
  (e: "update:modelValue", value: boolean): void
}>();

defineProps(['modelValue'])

const t = useT();
const picture = ref([]);

const handlerClear = () => {
  emits("editReset");
}

const submit = async () => {
  const form = new FormData();
  form.append("picture", picture.value[0]);
  await api.postUserPicture(form);
  useSuccessMessage(t("page.dashboard."))
}
</script>
