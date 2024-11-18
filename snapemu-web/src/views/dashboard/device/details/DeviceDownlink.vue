<template>
  <v-card class="mr-auto ml-auto h-2/3 pa-4" width="1230" elevation="6" rounded="xl">
    <v-row>
      <v-col cols="1" class="align-end">
        <span>data </span>
      </v-col>
      <v-col>
        <div class="border-2">
          <HexInput class="w-full pa-1" v-model="hexData" :length="100" />
        </div>
      </v-col>
    </v-row>
    <v-row justify="end" align-content="center" class="pl-1 pr-1">
      <v-col cols="1">
        <v-btn @click="sendDownHandler" class="mr-2">{{
            t('page.dashboard.device.downlink.send')
          }}</v-btn>
      </v-col>

      <v-col cols="1">
        <v-btn @click="showSavePage = true">{{ t('page.dashboard.device.downlink.save') }}</v-btn>
      </v-col>
      <v-col cols="1">
        <v-checkbox v-model="showEditPage" >
        <span>
          {{
            showEditPage
              ? t('page.dashboard.device.downlink.in_edit')
              : t('page.dashboard.device.downlink.edit')
          }}
        </span>
        </v-checkbox>
      </v-col>
    </v-row>
    <v-row v-if="showEditPage">
      <template v-for="data in templateArr" :key="data.id">
        <v-btn class="mr-4 !normal-case" @click="editTemplateHandler(data)" :text="data.name">
        </v-btn>
      </template>
    </v-row>
    <v-row v-else>
      <template v-for="data in templateArr" :key="data.id">
        <v-btn class="mr-4 !normal-case" @click="sendTemplateHandler(data)" :text="data.name">
        </v-btn>
      </template>
    </v-row>
    <v-dialog max-width="500" persistent v-model="showSavePage">
      <template v-slot:default>
        <v-card title="Dialog">
          <v-card-text>
            <v-row>
              <div class="w-full ma-1">
                <span>data </span>
                <div class="border-black border-2">
                  <HexInput class="w-full pa-1" v-model="hexData" :length="100" />
                </div>
              </div>
              <div class="w-full ma-1">
                <v-text-field
                  v-model="currentTempName"
                  label="Name"
                  variant="outlined"
                ></v-text-field>
              </div>
            </v-row>
          </v-card-text>

          <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn
              v-if="showEditPage"
              :text="t('page.dashboard.device.downlink.delete')"
              @click="deleteHandler"
            ></v-btn>
            <v-btn
              :text="t('page.dashboard.device.downlink.cancel')"
              @click="showSavePage = false"
            ></v-btn>
            <v-btn
              v-if="!showEditPage"
              :text="t('page.dashboard.device.downlink.save')"
              @click="saveAsHandler"
            ></v-btn>
          </v-card-actions>
        </v-card>
      </template>
    </v-dialog>
  </v-card>
</template>

<script setup lang="ts">
import { useRoute } from 'vue-router'
import HexInput from '@/components/widget/HexInput.vue'
import { onMounted, ref } from 'vue'
import { api } from '@/composables/api'
import { useSuccessMessage, useWarningMessage } from '@/composables/notify'
import { useT } from '@/composables/i18n'

const t = useT()
const templateArr = ref([])
const route = useRoute()
const deviceID = route.params.id as string

const hexData = ref('')
const currentTempName = ref('')
const currentTempPort = ref(2)
const showSavePage = ref(false)
const showEditPage = ref(false)
const currentEditItem = ref(null)

const reloadTemplate = async () => {
  let templates = []
  try {
    templates = (await api.getDownTemplate(deviceID)).data
  } catch (e) {
    console.log(e)
    return
  }
  templateArr.value = templates
}
onMounted(async () => {
  await reloadTemplate()
})

const editTemplateHandler = async (data) => {
  currentEditItem.value = data
  hexData.value = data.data
  currentTempName.value = data.name
  showSavePage.value = true
}
const sendDownHandler = async () => {
  if (hexData.value.length === 0) {
    useWarningMessage('data is empty')
    return
  }
  if (hexData.value.length % 2 !== 0) {
    useWarningMessage('The data must be hexadecimal')
    return
  }
  let data = ''
  for (let i = 0; i < hexData.value.length; i += 2) {
    let a = hexData.value.slice(i, i + 2)
    data += String.fromCharCode(Number.parseInt(a, 16))
  }
  try {
    await api.postDownLink(deviceID, btoa(data))
  } catch (e) {
    useWarningMessage(e.message)
    return
  }
  useSuccessMessage('Schedule completion')
}

const saveAsHandler = async () => {
  try {
    await api.postTemplate(deviceID, currentTempName.value, currentTempPort.value, hexData.value)
  } catch (e) {
    useWarningMessage(e.message)
    return
  }
  await reloadTemplate()
  showSavePage.value = false
}

const sendTemplateHandler = async (inputData: { data: string }) => {
  if (inputData.data.length === 0) {
    useWarningMessage('data is empty')
    return
  }
  if (inputData.data.length % 2 !== 0) {
    useWarningMessage('The data must be hexadecimal')
    return
  }
  let data = ''
  for (let i = 0; i < inputData.data.length; i += 2) {
    let a = inputData.data.slice(i, i + 2)
    data += String.fromCharCode(Number.parseInt(a, 16))
  }
  try {
    await api.postDownLink(deviceID, btoa(data))
  } catch (e) {
    useWarningMessage(e.message)
    return
  }
  useSuccessMessage('Schedule completion')
}

const deleteHandler = async () => {
  try {
    await api.deleteTemplate(deviceID, currentEditItem.value.id)
  } catch (e) {
    useWarningMessage(e.message)
    return
  }
  await reloadTemplate()
  showSavePage.value = false
}
</script>
