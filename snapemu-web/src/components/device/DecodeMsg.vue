<template>
  <v-sheet>
    <v-row>
      <v-col
        :cols="result.length === 1 ? '12' : '6'"
        v-for="(item, index) in result"
        :key="index"
      >
      <DataCard
        :name="props.script.map[item.id].d_name"
        :value="item.data"
        :unit="props.script.map[item.id].d_unit"
        :only="result.length === 1"
        :color="indexToBg(index)"
        :v_type="props.script.map[item.id].d_type"
      />

      </v-col>
    </v-row>
  </v-sheet>
</template>

<script setup lang="ts">

import DataCard from '@/views/dashboard/device/DataCard.vue'
import { indexToBg } from '@/utils/define'
import { computed } from 'vue'

const props = defineProps<{
  script: {
      name: string,
      script: string,
      map: any[]
  },
  res: {
    result: any,
    state: boolean
  }
}>();

const result = computed(() => {
  if (props.res.state) {
    return JSON.parse(props.res.result).data;
  } else {
    return [];
  }
});

</script>

<style lang="scss" scoped>
</style>
