<template>
  <div>
    <v-text-field
      :readonly="true"
      :type="!password ? 'text' : 'password'"
      :model-value="showText"
      :append-icon="!password ? 'mdi-eye' : 'mdi-eye-off'"
      @click:append="password = !password"
      variant="outlined"
    >
      <template #append-inner v-if="isSupported">
        <v-icon icon="mdi-clipboard-check-outline" v-if="copied" @click.stop="copyText">
        </v-icon>
        <v-icon icon="mdi-clipboard-multiple-outline" v-else @click.stop="copyText">
        </v-icon>
      </template>
    </v-text-field>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useClipboard } from '@vueuse/core'

const props = defineProps<{
  password: boolean,
  text: string
}>();

const password = ref(props.password);

const copied = ref(false);
const showText = computed(() => {
  return props.text
})

const { copy, isSupported } = useClipboard({ source: props.text })
const copyText = () => {
  copy(props.text);
  copied.value = true;
}

</script>
