<template>
  <v-layout>

    <v-navigation-drawer
      v-model="drawer"
      rounded="xl"
      width="300"
      elevation="8"
    >
      <div class="d-flex flex-col justify-between p-2 h-full text-black">
        <div>
          <div class="h-14">
            <v-img
              class="m-auto"
              width="200px"
              :src="WebIcon"
            />
          </div>
          <RouterLink class="item"  :class="{ active: getActive('') }" to="/dashboard/">
            <v-list-item class="w-full" color="rgb(10,129,127)" :prepend-icon="OverViewIcon" :append-icon="ArrowSvg" :title="t('page.dashboard.overview.overview')" value="overview"></v-list-item>
          </RouterLink>
          <v-divider class="border-opacity-50" color="error" :inset="true"></v-divider>
          <RouterLink class="item" to="/dashboard/device/">
            <v-list-item class="w-full"  color="rgb(10,129,127)" :class="{ active: getActive('device') }" :prepend-icon="getActive('device') ? DeviceIconSelected : DeviceIcon" :append-icon="ArrowSvg" :title="t('page.dashboard.overview.device')" value="device"></v-list-item>
          </RouterLink>
          <template v-if="isDevicePage">
            <RouterLink to="index" >
              <v-list-item color="rgb(10,129,127)"  :prepend-icon="EmptyIcon" value="device">
                <v-list-item-title>
                  <span :class="{ activeItem: getDeviceActive('index') }">
                  {{ t('page.dashboard.device.index') }}
                  </span>
                </v-list-item-title>
              </v-list-item>
            </RouterLink>
            <RouterLink to="logs">
              <v-list-item color="rgb(10,129,127)" :prepend-icon="EmptyIcon" value="device">
                <v-list-item-title>
                  <span :class="{ activeItem: getDeviceActive('logs') }">
                  {{ t('page.dashboard.device.logs.logs') }}
                  </span>
                </v-list-item-title>
              </v-list-item>
            </RouterLink>
            <RouterLink to="info">
              <v-list-item color="rgb(10,129,127)" :prepend-icon="EmptyIcon" value="device">
                <v-list-item-title>
                  <span :class="{ activeItem: getDeviceActive('info') }">
                  {{ t('page.dashboard.device.info.info') }}
                  </span>
                </v-list-item-title>
              </v-list-item>
            </RouterLink>
            <RouterLink to="decode">
              <v-list-item color="rgb(10,129,127)" :prepend-icon="EmptyIcon" value="device">
                <v-list-item-title>
                  <span :class="{ activeItem: getDeviceActive('decode') }">
                  {{ t('page.dashboard.device.decode.decode') }}
                  </span>
                </v-list-item-title>
              </v-list-item>
            </RouterLink>
            <RouterLink to="downlink">
              <v-list-item color="rgb(10,129,127)" :prepend-icon="EmptyIcon" value="device">
                <v-list-item-title>
                  <span :class="{ activeItem: getDeviceActive('downlink') }">
                  {{ t('page.dashboard.device.downlink.downlink') }}
                  </span>
                </v-list-item-title>
              </v-list-item>
            </RouterLink>
          </template>
          <v-divider class="border-opacity-50" color="error" :inset="true"></v-divider>
          <RouterLink class="item" to="/dashboard/integration/">
            <v-list-item class="w-full" color="rgb(10,129,127)" :class="{ active: getActive('integration') }" :prepend-icon="IntegrationIcon" :append-icon="ArrowSvg" :title="t('page.dashboard.overview.integration')" value="integration"></v-list-item>
          </RouterLink>
          <v-divider class="border-opacity-50" color="error" :inset="true"></v-divider>
          <RouterLink class="item" to="/dashboard/group/">
            <v-list-item class="w-full" color="rgb(10,129,127)" :class="{ active: getActive('group') }" :prepend-icon="GroupIcon" :append-icon="ArrowSvg" :title="t('page.dashboard.overview.group')" value="group"></v-list-item>
          </RouterLink>
          
          <Teleport to="body">
            <div class="fixed toggle-tab z-[10000]" :style="style" ref="el">
              <v-btn
                @click.stop="reversalNavigation"
                :icon="true"
                text="S"
              ></v-btn>
            </div>
          </Teleport>
        </div>
        <div>
          <Info />
        </div>
      </div>
    </v-navigation-drawer>
    <v-main :scrollable="true">
      <RouterView />
    </v-main>
  </v-layout>
</template>

<script setup lang="ts">

import WebIcon from '@/assets/icon/snap-emu-log-icon.webp';

import { useT } from '@/composables/i18n'
import { useDraggable } from '@vueuse/core'
import Lang from '@/components/user/SiteLang.vue'
import Info from '@/components/user/UserInfo.vue'
import { useSideNavigation } from '@/composables/setting'
import { useRoute } from 'vue-router'
import { computed, ref } from 'vue'
import { useCacheStore } from '@/stores/cache'
import { api } from '@/composables/api'
import { useSettingStore } from '@/stores/setting'
import { isUndefined } from 'lodash-es'
import { useSuccessMessage, useWarningMessage } from '@/composables/notify'
import ArrowSvg from '@/components/widget/ArrowSvg.vue'
import OverViewIcon from '@/components/icon/OverViewIcon.vue'
import DeviceIcon from '@/components/icon/DeviceIcon.vue'
import IntegrationIcon from '@/components/icon/IntegrationIcon.vue'
import GroupIcon from '@/components/icon/GroupIcon.vue'
import EmptyIcon from '@/components/icon/EmptyIcon.vue'
import DeviceIconSelected from '@/components/icon/DeviceIconSelected.vue'

const t = useT();
const setting = useSettingStore();
const drawer = useSideNavigation();
const el = ref<HTMLElement | null>(null)
const clickFlag: {
  click: boolean,
  timerHandler?: number
} = {
  click: true,
  timerHandler: undefined
};

const freeClickFlag = () => {
  clickFlag.click = true;
  clickFlag.timerHandler = undefined;
}

const { style } = useDraggable(el, {
  onMove() {
    clickFlag.click = false;
    if (!isUndefined(clickFlag.timerHandler)) {
      clearTimeout(clickFlag.timerHandler);
    }
    clickFlag.timerHandler = setTimeout(freeClickFlag, 200) as unknown as number;
  },
  onEnd(position) {
    setting.updateSidePosition(position.x, position.y);
  },
  initialValue: { x: setting.sideX, y: setting.sideY },
})
const route = useRoute();

const isDevicePage = computed(() => {
  const devicePath = '/dashboard/device/';
  return route.path.startsWith(devicePath) && route.path.length != devicePath.length
})

const reversalNavigation = () => {
  if (clickFlag.click) {
    drawer.value = !drawer.value;
  }
}

const log = async () => {
  useSuccessMessage(`message: ${new Date()}`)
}
const errlog = async () => {
  useWarningMessage(`message: ${new Date()}`)
}
const getActive = (s: string )=> {
  return route.path.split('/')[2] === s;
}

const getDeviceActive = (s: string )=> {
  return route.path.split('/')[4] === s;
}

</script>


<style scoped lang="scss">

.item {
  height: 54px;
  display: flex;
  align-content: center;
  width: 100%;
}

.active {
  background-color: #0D5F5D;
  border-radius: 17px;
  color: white;
}

.activeItem {
  border-bottom: 2px solid #0D5F5D;
}
</style>
