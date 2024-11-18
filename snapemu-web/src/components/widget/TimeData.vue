<template>
  <div>
    <v-chart 
      @click="click"
      class="chart" 
      :option="option" />
  </div>
</template>

<script setup lang="ts">
import { use } from 'echarts/core';
import { LineChart } from 'echarts/charts';
import { TitleComponent, GridComponent, DataZoomComponent, LegendComponent, DatasetComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { computed, provide, ref } from 'vue'
import type { ComposeOption } from 'echarts/core';
import type { LineSeriesOption } from 'echarts/charts';
import type {
  TitleComponentOption,
  GridComponentOption
} from 'echarts/components';
import VChart, { THEME_KEY } from 'vue-echarts';

use([
  CanvasRenderer,
  TitleComponent,
  LegendComponent,
  DatasetComponent,
  TooltipComponent,
  LineChart,
  GridComponent,
  DataZoomComponent
]);


const props = defineProps<{
  color: string,
  data: {
    time: number,
    data: any
  }[]
}>()

const emits = defineEmits<{
  (e: "dataSelect", index: number): void
}>()

type EChartsOption = ComposeOption<
  | TitleComponentOption
  | GridComponentOption
  | LineSeriesOption
>

const click = (param: { dataIndex: number }) => {
  emits("dataSelect", param.dataIndex)
}

provide(THEME_KEY, 'light');

const option = ref<EChartsOption>({
  animationDuration: 1000,
  tooltip: {
    trigger: 'axis',
    axisPointer: { type: 'cross' }
  },
  xAxis: {
    type: 'time',
  },
  yAxis: {},
  dataset: {
    dimensions: ["time", "data"],
    source: props.data
  },
  dataZoom: [
        {
          type: 'inside',
          start: 90,
          end: 100
        },
  ],
  series: [
    {
      type: 'line',
      smooth: true,
      lineStyle: {
        shadowColor: 'rgba(166,141,151,0.5)',
        shadowBlur: 2,
        shadowOffsetY: 10,
      },
      areaStyle: {
        color: props.color,
        opacity: 0.5
      }
    }
  ]
})

</script>

<style lang="scss">
.chart {
  height: 380px;
  width: 650px;
}
</style>
