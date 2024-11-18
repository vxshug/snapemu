<template>
  <div class="px-1">
    <v-select v-model="langLocale" :items="items"></v-select>
  </div>
</template>

<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { useSettingStore } from '@/stores/setting'
import { computed } from 'vue'
import { useRouter } from 'vue-router'

const items = [
  {
    title: '简体中文',
    value: 'zh',
  },
  {
    title: 'English',
    value: 'en',
  },
]

const setting = useSettingStore();
const { locale } = useI18n()
const router = useRouter();

const langLocale = computed({
  get() {
    return locale.value;
  },
  set(newValue) {
    locale.value = newValue;
    setting.updateLanguage(newValue);
    router.go(0);
  }
})
</script>
