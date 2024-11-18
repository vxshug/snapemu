<script setup lang="ts">
const props = defineProps<{
  time: number
}>();
</script>

<template>
  <span>
    {{ new Date(props.time).toLocaleTimeString() }}
  </span>
</template>

<style scoped lang="scss">

</style>