import { defineStore } from 'pinia'
import { type Ref, ref } from 'vue'
import { useRequest } from '@/composables/request'
import type { UserInfo } from '@/type/response'

export const useCacheStore = defineStore('cache', () => {
  const userInfo = ref<UserInfo>();

  const count = ref(0);
  const countInc = () => count.value++;
  async function deviceGroup() {
  }

  async function getUserInfo(): Promise<Ref<UserInfo>> {
    if (typeof userInfo.value === 'undefined') {
      userInfo.value = await useRequest<UserInfo>({
        path: "/api/v1/user/info",
        method: "GET",
        noRedirect: true
      }, {
        snackBar: true
      });
    }
    return userInfo as Ref<UserInfo>;
  }
  function invalidUserInfo() {
    userInfo.value = undefined;
  }

  return { userInfo, count, countInc,  deviceGroup, getUserInfo, invalidUserInfo }
})
