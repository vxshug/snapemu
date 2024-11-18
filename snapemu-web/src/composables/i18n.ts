import { useI18n as i18n } from 'vue-i18n'
import type { MessageSchema, SupportLang } from '@/main'

export const useI18n = () => {
  return i18n<{ message: MessageSchema }, SupportLang>()
}
export const useT = () => {
  return i18n<{ message: MessageSchema }, SupportLang>().t
}