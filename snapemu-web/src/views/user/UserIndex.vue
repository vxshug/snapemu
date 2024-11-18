<script setup lang="ts">
import WebIcon from '@/assets/icon/snap-emu-log.350x350.webp';
</script>

<template>
  <v-layout>
    <v-main class="max-w-full max-h-full">
      <div>
        <v-img
          class="mx-auto my-6"
          width="120"
          :src="WebIcon"
        ></v-img>

        <v-card
          class="mx-auto pa-12 pb-8"
          elevation="8"
          max-width="448"
          rounded="lg"
        >
          <router-view></router-view>
        </v-card>
      </div>
    </v-main>
  </v-layout>
</template>

<style scoped>

</style>