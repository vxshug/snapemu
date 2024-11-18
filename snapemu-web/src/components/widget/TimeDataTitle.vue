<template>
  <v-card
    class="mx-auto"
    width="223px"
    height="168px"
    :class="bg"
    :color="props.bg"
  >
    <v-card-item>
      <v-card-title>
        <span class="text-4xl">
          {{props.name}}
        </span>
      </v-card-title>
    </v-card-item>
    <v-card-text class="h-full d-flex align-center">
      <span class="text-4xl">
        {{dataValue}}
      </span>
      <span class="text-4xl">
        {{props.unit}}
      </span>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">

import { computed } from 'vue'

const props = defineProps<{
  bg: string,
  name: string,
  unit: string,
  value: any
}>()

const dataValue = computed(() => ~~props.value == props.value ? props.value : props.value.toFixed(2))

const bg = computed(() => {
  return {
    backgroundColor: props.bg
  }
})

</script>

