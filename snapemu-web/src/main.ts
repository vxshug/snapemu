import './assets/main.css'

import { createApp } from 'vue'
import { createI18n, useI18n } from 'vue-i18n'

import 'vuetify/styles'
import { createVuetify } from 'vuetify'
import { createVueI18nAdapter } from 'vuetify/locale/adapters/vue-i18n'

import App from './App.vue'
import router from './router'

import '@mdi/font/css/materialdesignicons.css'

import en from '../locales/en.json'
import zh from '../locales/zh.json'

import { aliases, mdi } from 'vuetify/iconsets/mdi'
import { useSettingStore } from '@/stores/setting'
import { store } from '@/stores'

export type SupportLang = 'zh' | 'en'
export type MessageSchema = typeof zh

const app = createApp(App)

app.use(store)
const setting = useSettingStore();
export const messages = {
  zh,
  en
}
const i18n = createI18n<[MessageSchema], SupportLang>({
  legacy: false,
  locale: setting.language,
  fallbackLocale: 'en',
  messages
})

const vuetify = createVuetify({
  ssr: true,
  locale: {
    adapter: createVueI18nAdapter({ i18n: i18n as any, useI18n })
  },
  icons: {
    defaultSet: 'mdi',
    aliases,
    sets: {
      mdi
    }
  }
})


app.use(router)

app.use(vuetify)
app.use(i18n)

app.mount('#app')
