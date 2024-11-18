import { defineStore } from 'pinia'
import { useStorage } from '@vueuse/core'
import { ref } from 'vue'

export const useSettingStore = defineStore('setting', () => {
  const token = ref(useStorage("access_token",""));
  const language = ref(useStorage("language","en"));
  const side = ref(useStorage("side",false));
  const sideX = ref(useStorage("sideX",40));
  const sideY = ref(useStorage("sideY",540));
  function updateSidePosition(x: number, y: number) {
    sideX.value = x;
    sideY.value = y;
  }
  function updateToken(n: string) {
    token.value = n;
  }
  function updateLanguage(lang: string) {
    language.value = lang;
  }

  function updateSide(display: boolean) {
    side.value = display;
  }

  return { sideX, sideY, token, language, side, updateSidePosition, updateToken, updateLanguage, updateSide }
})
