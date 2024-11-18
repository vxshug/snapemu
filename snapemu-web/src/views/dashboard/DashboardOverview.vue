<template>
  <v-sheet class="mt-40">
    <v-container
      class="mb-6"
    >
      <v-row
        align="start"
        :no-gutters="true"
      >
        <v-col>
          <v-sheet class="pa-2 ma-2 text-center text-3xl">
            {{$t("page.dashboard.overview.welcome")}}
          </v-sheet>
        </v-col>
      </v-row>
      <v-row>
        <v-col cols="2">
        </v-col>
        <v-col>
          <RouterLink
            to="/dashboard/device"
          >
            <v-card
              height="300px"
              width="300px"
            >
              <v-img
                class="mx-auto mt-12"
                width="180px"
                :src="device"
              />
            </v-card>
          </RouterLink>
        </v-col>
        <v-col>
          <RouterLink
            to="/dashboard/device"
          >
            <v-card
              width="300px"
              height="300px"
            >
              <v-img
                class="mx-auto mt-12"
                width="180px"
                :src="gateway"
              />
            </v-card>
          </RouterLink>
        </v-col>
        <v-col cols="2">
        </v-col>
      </v-row>

    </v-container>
  </v-sheet>
</template>

<script setup lang="ts">

import device from "@/assets/icon/device.svg"
import gateway from "@/assets/icon/gateway.svg"

</script>
