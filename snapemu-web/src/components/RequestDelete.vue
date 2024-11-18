<script setup lang="ts">
import { ref } from 'vue'
const email = ref("")
const code = ref("")
const password = ref("")

const getValidCode = async (e: Event) => {
  e.preventDefault()
  const json = await (await fetch("https://test.snapemu.com/api/v1/user/delete/request", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      "email": email.value.trim()
    })
  })).json() as {
    code: number,
    data: string,
    message: string
  };
  if (json.code !== 0) {
    console.error("Error occurred while deleting request", json.message)
    return;
  }
  alert(json.data)
}

const submit = async (e: Event) => {
  e.preventDefault()
  const json = await (await fetch("https://test.snapemu.com/api/v1/user/delete", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      email: email.value.trim(),
      code: code.value.trim(),
      password: password.value.trim(),
    })
  })).json() as {
    code: number,
    data: string,
    message: string
  };
  if (json.code !== 0) {
    console.error("Error occurred while deleting request", json.message)
    return;
  }
  alert(json.data)
}

</script>

<template>
  <v-layout>
    <v-main class="max-w-full max-h-full">
        <v-card
          class="mx-auto pa-8 pb-8"
          elevation="8"
          max-width="500"
          rounded="lg"
        >
          <h1>please input email</h1>
          <form @submit="submit">
            <v-col>
              <v-text-field label="email" placeholder="email" type="email" v-model="email" />
              <v-row align-content="center" justify="space-between">
                <v-text-field class="p-3" type="text" label="code" placeholder="code" required v-model="code" />
                <v-btn class="mt-4 ml-3" variant="outlined" @click="getValidCode">get code</v-btn>
              </v-row>
              <v-text-field  type="password" label="password" placeholder="password" required v-model="password" />
              <v-btn type="submit">Submit</v-btn>
            </v-col>
          </form>
        </v-card>
    </v-main>
  </v-layout>

</template>

<style scoped lang="scss">

</style>