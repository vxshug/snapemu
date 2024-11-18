<script setup lang="ts">

import { computed } from 'vue'

const emits = defineEmits<{
  (e: 'close'): void
}>();

const props = withDefaults(defineProps<{
    status?: 'warning' | 'success',
    text?: string,
}>(), {
  status: 'success',
  text: ''
});

const statusSuccessIcon = computed(() => props.status === 'success')
const styleObj = computed(() => ({
  backgroundColor: statusSuccessIcon.value ? 'rgb(76,175,80)' : 'rgb(251,140,0)'
}))

</script>

<template>
  <div class="d-flex p-2 rounded-lg cursor-pointer" :style="styleObj" @click="emits('close')">
    <i class="h-7 w-7 text-2xl d-block ">
      <svg v-if="statusSuccessIcon" class="fill-current text-white" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" role="img" aria-hidden="true">
        <path d="M12 2C6.5 2 2 6.5 2 12S6.5 22 12 22 22 17.5 22 12 17.5 2 12 2M10 17L5 12L6.41 10.59L10 14.17L17.59 6.58L19 8L10 17Z"></path>
      </svg>
      <svg v-else class="fill-current text-white" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" role="img" aria-hidden="true">
        <path d="M13,13H11V7H13M13,17H11V15H13M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2Z"></path>
      </svg>
    </i>
    <div class="text-1xl">
      <div class="p-1">{{props.text}}</div>
    </div>
  </div>
</template>

<style scoped lang="scss">

</style>