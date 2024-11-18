<template>
  <v-card
    class="text-white d-flex align-center rounded-xl !border-2 hover:!border-stone-50"
    height="80px"
    :color="props.color"
  >
    <v-card-item class="data-font w-full">
      <div :class="{ dataOnly: props.only }" class="flex justify-space-between">
        <div>
          {{props.name}}
        </div>
        <div v-if="isBool">
          <v-switch
            :value="props.value"
            hide-details
            :disabled="true"
            :inset="true"
          ></v-switch>
        </div>
        <div v-else>
          <span>{{data.data}}</span>
          <sup class="text-h6">{{props.unit}}</sup>
        </div>
      </div>
    </v-card-item>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { isBoolean } from 'lodash-es'

const props = defineProps<{
  name: string,
  value: any,
  unit: string,
  only: boolean,
  color: string
}>()

const isBool = computed(() => isBoolean(props.value));

const data = computed(() => {
  return {
    data: props.value.toFixed(2)
  }
})
</script>

<style lang="scss" scoped>
.data-font {
  font-size: 24px;
}

.dataOnly {
  display: flex;
  justify-content: space-between;
}
</style>
