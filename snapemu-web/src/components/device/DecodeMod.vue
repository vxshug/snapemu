<template>
  <div class="h-full">
  <v-text-field v-model="model.name"  :label="$t('page.dashboard.device.decode.script_name')"></v-text-field>
  <CodeMap v-model="model.map" />
    <CodeEditor v-model="model.script" javascript />
  </div>
</template>

<script setup lang="ts">
import CodeEditor from '@/components/widget/CodeEditor.vue'
import CodeMap from '@/components/device/CodeMap.vue'

const model = defineModel<{
  name: string,
  script: string,
  map: any[]
}>({ required: true });

</script>

<style lang="scss" scoped>
</style>
