<template>
  <v-card
    class="mx-auto"
    width="1168px"
  >
    <v-row justify="center" align="center">
      <TimeData
        @dataSelect="titleDataHandler"
        :data="props.data.data"
        :color="props.titleBg"
      />
      <TimeDataTitle
        :name="props.data.name"
        :unit="props.data.unit"
        :bg="props.titleBg"
        :value="titleData"
      >
      </TimeDataTitle>
    </v-row>
  </v-card>
</template>

<script setup lang="ts">
import TimeDataTitle from '@/components/widget/TimeDataTitle.vue'
import TimeData from '@/components/widget/TimeData.vue'
import { ref } from 'vue'

interface Data {
  name: string,
  unit: string,
  v_type: string,
  data_id: number
  data: {
    time: number,
    data: any
  }[]
}
const props = defineProps<{
  data: Data;
  titleBg: string;
}>()

const titleData = ref(props.data.data[props.data.data.length -1].data);

const titleDataHandler = (index: number) => {
  titleData.value = props.data.data[index].data;
}

</script>

