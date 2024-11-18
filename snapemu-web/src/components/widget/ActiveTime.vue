<template>
  <span>
    {{secondTime}}
  </span>
</template>

<script setup lang="ts">
import { useT } from '@/composables/i18n'
import { computed } from 'vue'

const props = defineProps<{
  time?: number
}>()
const t = useT();

const secondTime = computed(() => {
  if (props.time) {
    const second = Math.round((new Date().getTime() - props.time) / 1000);
    if (second < 60) {
      return t("page.dashboard.device.time.second", { time: second });
    }
    if (second < 3600) {
      const minute = Math.round(second / 60);
      return t("page.dashboard.device.time.minute", { time: minute });
    }
    if (second < 86400) {
      const hour = Math.round(second / 3600);
      return t("page.dashboard.device.time.hour", { time: hour });
    }
    if (second < 1209600) {
      const day = Math.round(second / 86400);
      return t("page.dashboard.device.time.day", { time: day });
    }
    return t("page.dashboard.device.time.timeout");
  }
  return t("page.dashboard.device.time.never");
});

</script>
