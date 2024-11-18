<template>
  <v-dialog width="500" :model-value="props.show" :close-on-back="false" :persistent="true">
    <v-card :title="$t('page.dashboard.device.delete')">
      <v-card-text>
        {{ $t("page.dashboard.device.delete_dialog", [props.deleteName]) }}
      </v-card-text>

      <v-card-actions>
        <v-spacer></v-spacer>
        <v-btn
          :text="$t('page.common.cancel')"
          @click="cancel"
        ></v-btn>
        <v-btn
          :text="$t('page.common.confirm')"
          @click="confirm"
        ></v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">

const props = defineProps<{
  show: boolean,
  deleteName: string,
  cancel?: () => void
  confirm?: () => void
}>();

const confirm = () => {
  if (props.confirm) {
    props.confirm();
  }
}

const cancel = () => {
  if (props.cancel) {
    props.cancel();
  }
}

</script>

<style lang="scss" scoped>
</style>
