<template>
  <codemirror
    v-model="model"
    placeholder="Code goes here..."
    :style="{ height: '200px' }"
    :autofocus="true"
    :indent-with-tab="true"
    :tab-size="2"
    :disabled = props.disabled
    :extensions="extensions"
    @ready="emits('ready')"
  />
</template>

<script setup lang="ts">
  import { Codemirror } from 'vue-codemirror'
  import { javascript } from '@codemirror/lang-javascript'
  import { json } from '@codemirror/lang-json'
  import { cpp } from '@codemirror/lang-cpp'
  import { oneDark } from '@codemirror/theme-one-dark'

  const model = defineModel<string>({ required: true });

  const props = withDefaults(defineProps<{
    javascript: boolean,
    json: boolean,
    cpp: boolean,
    disabled: boolean,
  }>(), {
    json: false,
    javascript: false,
    cpp: false,
    disabled: false,
  });

  const emits = defineEmits<{
    (e: "ready"): void
  }>();

  const extensions: any[] = []

  if (props.json) {
    extensions.push(json())
  }
  if (props.javascript) {
    extensions.push(javascript())
  }
  if (props.cpp) {
    extensions.push(cpp())
  }
</script>
