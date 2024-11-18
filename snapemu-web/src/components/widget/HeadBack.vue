<script setup lang="ts">
import BackSvg from '@/components/widget/BackSvg.vue'
import router from '@/router'

const backPage = () => {
  router.back()
}

</script>

<template>
  <div style="height: 80px">
    <div style="width: 1230px;" class="ml-auto mr-auto">
      <div class="flex justify-end align-end pa-3" style="height: 80px">
        <BackSvg @click="backPage" />
      </div>
    </div>
  </div>
</template>

<style scoped lang="scss">

</style>