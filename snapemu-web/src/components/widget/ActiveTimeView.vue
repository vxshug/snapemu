<template>
  <span>
    {{secondTime}}
  </span>
</template>

<script setup lang="ts">
import { useT } from '@/composables/i18n'
import { computed } from 'vue'
import dayjs from 'dayjs'

const props = defineProps<{
  time?: number
}>()
const t = useT();

const secondTime = computed(() => {
  if (props.time) {
    const time = dayjs(props.time);
    let now = dayjs();
    if (now.isSame(time, 'day')) {
      return t("page.dashboard.device.time.last_active", { time: time.format('H:mm') });
    } else {
      const day = Math.ceil((now.valueOf() - time.valueOf()) / 86400000);
      return t("page.dashboard.device.time.day", { time: day });
    }
  }
  return t("page.dashboard.device.time.never");
});

</script>

<style lang="scss" scoped>

</style>
