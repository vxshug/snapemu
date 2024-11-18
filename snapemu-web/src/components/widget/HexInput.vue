<template>
  <input 
     class="font-semibold h-9"
     ref="inputRef"
     type="text" 
     autocomplete="off" 
     :value="defaultValue"
     @keydown="inputDownHandler" 
     @paste.prevent="pasteHandler"
     @compositionend.prevent="forbidInputMethod"
     @keyup="inputUpHandler"/>
</template>

<script setup lang="ts">

import { ref } from 'vue'
import { clearEmpty } from '@/utils/str'
import { isUndefined } from 'lodash-es'

const props = withDefaults(defineProps<{
  modelValue: string,
  length?: number
}>(), {
  length: 8
})

const emits = defineEmits<{
  (e: "update:modelValue", s: string): void
}>()
const regEx = /^Key[a-fA-F]|Digit[0-9]$/;
const regHexEx = /^[0-9a-fA-F]$/;

const defaultValue = ref(props.modelValue ? props.modelValue.replace(/(?<=^([0-9a-fA-F]{2})+)(?!$)/g, ' ') : '')
const valueLength = ref(props.length);
const inputRef = ref<HTMLInputElement>();
let shortCodeFlag = false;

const isPastePrefix = (code: string) => code.startsWith('Control') || code.startsWith('Meta');
const updateValue = (s: string) => {
  if (!isUndefined(inputRef.value)) {
    inputRef.value.value = s;
  }
}

const inputUpHandler = (event: KeyboardEvent) => {
  if (isPastePrefix(event.code)) {
    shortCodeFlag = false;
  }
}

const inputDownHandler = (event: KeyboardEvent) => {
  if (regEx.test(event.code)) {
    const noSpaceValue = clearEmpty(defaultValue.value);
    if (noSpaceValue.length < valueLength.value) {
      const code = event.code.slice(event.code.length - 1);
      if (noSpaceValue.length % 2 === 0 && noSpaceValue.length > 0) {
        defaultValue.value = defaultValue.value + ' ';
      }
      defaultValue.value = defaultValue.value + code;
    }
  } 
  if (event.code === 'Backspace') {
    defaultValue.value = defaultValue.value.slice(0, defaultValue.value.length - 1)
    if (defaultValue.value.endsWith(' ')) {
      defaultValue.value = defaultValue.value.slice(0, defaultValue.value.length - 1)
    } 
  }
  emits("update:modelValue", clearEmpty(defaultValue.value));

  if (isPastePrefix(event.code)) {
    shortCodeFlag = true;
    return false;
  }
  if (shortCodeFlag) {
    return false;
  }
  event.preventDefault();
  return false;
}

const pasteHandler = (event: ClipboardEvent) => {
  if (!event.clipboardData) {
    return;
  }
  const paste: string = event.clipboardData.getData("text");
  if (paste) {
    const arr = paste.split('');
    for (const item in arr) {
      const noSpaceValue = clearEmpty(defaultValue.value);
      if (noSpaceValue.length < valueLength.value) {
        if (regHexEx.test(arr[item])) {
          if (noSpaceValue.length % 2 === 0 && noSpaceValue.length > 0) {
            defaultValue.value = defaultValue.value + ' ';
          }
          defaultValue.value = defaultValue.value + arr[item].toUpperCase();
        }
        continue;
      }
      break;
    }
  }
  emits("update:modelValue", clearEmpty(defaultValue.value))
}

const forbidInputMethod = () => {
  updateValue(defaultValue.value);
}

</script>
