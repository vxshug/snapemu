<template>
  <div class="inline-block">
    <div v-if="noBattery">
      N/A
    </div>
    <div v-else class="electric-panel" :class="bgClass">
      <div class="panel">
        <div class="remainder" :style="{width: props.battery +'%'}" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">

import { computed } from 'vue'
import { isUndefined } from 'lodash-es'

const props = defineProps<{
  battery?: number
}>();

const noBattery = computed(() => isUndefined(props.battery))

const bgClass =  computed(() => {
  if (props.battery) {
    if (props.battery >= 30) {
      return 'success'
    } else if (props.battery >= 15) {
      return 'warning'
    } else if (props.battery >= 1) {
      return 'danger'
    } else {
      return 'danger'
    }
  }
  return ""
})
</script>

<style lang="scss" scoped>
$color-primary: #447ced;
$color-success: #13ce66;
$color-warning: #ffba00;
$color-danger: #ff4949;
$color-info: #909399;
$color-white: #fff;

@mixin panel($color) {
  .panel {
    border-color: #{$color};
    &:before {
      background: #{$color};
    }
    .remainder {
      background: #{$color};
    }
  }
  .text {
    color: #{$color};
  }
}
.electric-panel {
  display: flex;
  justify-content: flex-start;
  align-items: center;

  .panel {
    box-sizing: border-box;
    width: 30px;
    height: 14px;
    position: relative;
    border: 2px solid #ccc;
    padding: 1px;
    border-radius: 3px;

    &::before {
      content: '';
      border-radius: 0 1px 1px 0;
      height: 6px;
      background: #ccc;
      width: 4px;
      position: absolute;
      top: 50%;
      right: -4px;
      transform: translateY(-50%);
    }

    .remainder {
      border-radius: 1px;
      position: relative;
      height: 100%;
      width: 0%;
      left: 0;
      top: 0;
      background: #fff;
    }
  }

  .text {
    text-align: left;
    width: 42px;
  }

  &.success {
    @include panel($color-success);
  }

  &.warning {
    @include panel($color-warning);
  }

  &.danger {
    @include panel($color-danger);
  }
}
</style>

