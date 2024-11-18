<script setup lang="ts">
import { base64ToHex } from '@/utils/base64'

const props = defineProps<{
  bytes: string
}>();

const bytes = '0x' + base64ToHex(props.bytes).replace(/(?<=^([0-9a-fA-F]{2})+)(?!$)/g, ' 0x');
</script>

<template>
  <div>
    <span>
      payload
    </span>
    <span class="bg-teal-100 p-1 border-solid border">
      {{ bytes }}
    </span>
  </div>
</template>

<style scoped lang="scss">

</style>