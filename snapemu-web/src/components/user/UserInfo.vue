<template>
  <v-menu
    min-width="200px"
    rounded
  >
    <template v-slot:activator="{ props }">
      <div
        class="d-flex w-full justify-between"
        v-bind="props"
      >
        <div class="">
          <v-avatar
            color="brown"
            size="30"
          >
            <v-img :src="user.picture" />
          </v-avatar>
          {{user.username}}
        </div>
        <div>
          <v-img :src="SwitchSvg"/>
        </div>
      </div>
    </template>
    <v-card>
      <v-card-text>
        <div class="mx-auto text-center">
          <v-avatar
            color="brown"
          >
            <v-img
              :src="user.picture"
            />
          </v-avatar>
          <p>{{ user.username }}</p>
          <v-divider class="my-3"></v-divider>
          <v-dialog width="800">
           <template v-slot:activator="{ props }">
             <v-btn class="!normal-case" v-bind="props" variant="text"> {{ $t("page.dashboard.password")}}</v-btn>
           </template>
           <template v-slot:default="{ isActive }">
              <UserEdit  />
           </template>
          </v-dialog>
          <v-divider class="my-3"></v-divider>
          <v-dialog width="800">
           <template v-slot:activator="{ props }">
             <v-btn class="!normal-case" v-bind="props" variant="text"> {{ $t("page.dashboard.picture")}}</v-btn>
           </template>
           <template v-slot:default="{ isActive }">
              <UserPicture />
           </template>
          </v-dialog>
          <v-divider class="my-3"></v-divider>
          <v-btn class="!normal-case" @click="logOut" variant="text">
            {{ $t("page.dashboard.logout")}}
          </v-btn>
        </div>
      </v-card-text>
    </v-card>
  </v-menu>
</template>

<script lang="ts" setup>
import SwitchSvg from '@/assets/icon/switch.svg'
import { onMounted, ref } from 'vue'
import type { UserInfo } from '@/type/response'
import { api } from '@/composables/api'
import UserEdit from '@/components/user/UserEdit.vue'
import UserPicture from '@/components/user/UserPicture.vue'
import { useRouter } from 'vue-router'
import { useSettingStore } from '@/stores/setting'
import { useCacheStore } from '@/stores/cache'

const router = useRouter();
const setting = useSettingStore();
const { invalidUserInfo } = useCacheStore();
const user = ref<UserInfo>({ email: '', picture: '', username: '' })

const logOut = async () => {
  setting.updateToken('');
  invalidUserInfo();
  setTimeout(() => {
    router.push("/user/login");
  }, 200)
}

onMounted(async () => {
  user.value = (await api.getUserInfo()).value;
})


</script>
