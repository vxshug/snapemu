<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  devAddr: string
}>();

const deviceAddr = computed(() => {
  return props.devAddr.replace(/(?<=^([0-9a-fA-F]{2})+)(?!$)/g, ' ')
})

</script>

<template>
  <div>
    <span>
      DevAddr:
    </span>
    <span class="bg-teal-100 p-1 border-solid border">
      {{deviceAddr}}
    </span>
  </div>
</template>

<style scoped lang="scss">

</style>