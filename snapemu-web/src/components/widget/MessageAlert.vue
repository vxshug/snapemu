<script setup lang="ts">

import type { AlertMessage } from '@/type/component'
import { computed, reactive, ref } from 'vue'
import MessageBox from '@/components/widget/MessageBox.vue'

type AlertMessageRemove = AlertMessage & {
  remove: boolean
}

const props = withDefaults(defineProps<{
  timeout?: number
}>(), {
  timeout: 5
});

const animationTime = computed(() => `${props.timeout * 0.2}s`);
const alertTimeOut = computed(() => props.timeout * 800);
const alertRemoveTime = computed(() => props.timeout * 200);

const counterHandler = ref(0);

const getCounter = () => {
  return counterHandler.value++;
}

const messages = reactive<AlertMessageRemove[]>([])

const clearAlert = (handler: number) => {
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    if (message.handler === handler) {
      message.remove = true;
    }
  }
  setTimeout(() => {
    let removeIndex = -1;
    for (let i = 0; i < messages.length; i++) {
      const message = messages[i];
      if (message.handler === handler) {
        removeIndex = i;
      }
    }
    if (removeIndex >= 0) {
      messages.splice(removeIndex, 1);
    }
  }, alertRemoveTime.value);
}

const messageInsert = (message: string, status: 'success' | 'warning' ) => {
  const handler = getCounter();
  messages.unshift({message, status, handler, remove: false})
  setTimeout(
    () => clearAlert(handler),
    alertTimeOut.value
  );
  return handler;
}

const successAlert = (message: string) => {
  return messageInsert(message, 'success');
}
const warningAlert = (message: string) => {
  return messageInsert(message, 'warning');
}


defineExpose({
  successAlert,
  warningAlert,
  clearAlert
})

</script>

<template>
    <div class="w-25 position-fixed right-0 z-[9999]">
      <MessageBox
        class="mt-4"
        :class="{ 'alert-message-in': !item.remove, 'alert-message-out': item.remove }"
        v-for="item in messages"
        :key="item.handler"
        :status="item.status"
        :text="item.message"
        @close="clearAlert(item.handler)"
      >
      </MessageBox>
    </div>
</template>

<style scoped lang="scss">
.alert-message-in {
  animation: v-bind(animationTime) 1 alternate slidein;
}
.alert-message-out {
  animation: v-bind(animationTime) 1 alternate slideout;
}
@keyframes slidein {
  from {
    transform: translateX(100%);
  }

  to {
    transform: translateX(0%);
  }
}
@keyframes slideout {
  from {
    transform: translateX(0%);
  }

  to {
    transform: translateX(100%);
  }
}
</style>