<template>
  <v-card class="relative !overflow-scroll mr-auto ml-auto h-2/3" width="1230" elevation="6" rounded="xl">
    <v-list lines="one" class="h-full">
      <v-list-item>
        <v-row>
          <v-col cols="1">
            <span class="text-2xl text-black">time</span>
          </v-col>
          <v-col cols="1">
            <span class="text-2xl text-black">type</span>
          </v-col>
          <v-col>
            <span class="text-2xl text-black">data</span>
          </v-col>
        </v-row>
      </v-list-item>
      <v-list-item
        @click="clickEvent(n)"
        v-for="n in logs"
        :key="n"
      >
        <DeviceEvent :event="n"/>
      </v-list-item>
    </v-list>
    <v-sheet ref="eventDomRef" class="absolute right-0 top-0 h-full" :style="styleEvent">
      <CodeEditor v-model="code" :json="true" :javascript="false" />
    </v-sheet>
  </v-card>
</template>

<script setup lang="ts">
import { type EventSourceEventMap, EventSourcePolyfill } from 'event-source-polyfill'
import { useRoute } from 'vue-router'
import { onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { useSettingStore } from '@/stores/setting'
import DeviceEvent from '@/components/device/DeviceEvent.vue'
import { urlBase } from '@/composables/request'
import type { DeviceEventRes } from '@/type/event'
import CodeEditor from '@/components/widget/CodeEditor.vue'
import { useAnimate } from '@vueuse/core'
import HeadBack from '@/components/widget/HeadBack.vue'
const route = useRoute()
const deviceID = route.params.id as string;
const logs = ref<DeviceEventRes[]>([]);
const setting = useSettingStore();
let evtSource: EventSourcePolyfill | null = null;
const code = ref('');
const currentEvent = ref();
const eventDomRef = ref();
const reconnectFlag = ref(true);

const styleEvent = reactive({
  transform: 'translateX(100%)',
  display: 'none',
});

const {
  reverse,
  playState
} = useAnimate(eventDomRef,[
    { transform: "translateX(0)" },
    { transform: "translateX(100%)" },
  ],
  {
    immediate: false,
    duration: 1000,
  })

watch(playState, (state) => {
  if (state === 'finished') {
    if (currentEvent.value === undefined) {
      styleEvent.transform = 'translateX(100%)';
    } else {
      styleEvent.transform = 'translateX(0)';
    }
  }
})

const clickEvent = (event: DeviceEventRes) => {
  if (styleEvent.display === 'none') {
    styleEvent.display = 'block';
  }
  if (currentEvent.value === undefined) {
    currentEvent.value = event;
    code.value = JSON.stringify(event, null, 2);
    reverse();
  } else if (currentEvent.value === event) {
    currentEvent.value = undefined;
    reverse();
  } else {
    currentEvent.value = event;
    code.value = JSON.stringify(event, null, 2);
  }
}

onMounted(() => {
  const messageHandler = (e: EventSourceEventMap['message']) => {
    const obj = JSON.parse(e.data) as DeviceEventRes;
    if (logs.value.length < 100) {
      logs.value.unshift(obj);
    } else {
      logs.value.unshift(obj);
      logs.value.pop();
    }
  };

  const connectHandler = () => {
    if (evtSource) {
      evtSource.close();
      evtSource = null;
      reconnectFlag.value = true;
    }
    if (reconnectFlag.value) {
      reconnectFlag.value = false;
      setTimeout(() => {
        try {
          evtSource = new EventSourcePolyfill(urlBase() + "/api/v1/device/log/" + deviceID, {
            headers: {
              'authorization': "Bearer " + setting.token
            }
          });
          evtSource.addEventListener("error", connectHandler)
          evtSource.addEventListener("message", messageHandler);
        } catch (e) {
          console.log("nnnnn")
          console.log(e)
        }
      }, 1000)
    }
  };
  connectHandler();
})

onUnmounted(() => {
  if (evtSource) {
    evtSource.close()
    evtSource = null;
  }
})
</script>
