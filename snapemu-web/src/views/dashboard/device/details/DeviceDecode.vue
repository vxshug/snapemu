<template>
  <v-card class="mr-auto ml-auto h-4/5 pa-4" width="1230" elevation="6" rounded="xl">
    <v-checkbox v-model="isJSDecode" label="JS decode"></v-checkbox>
    <template v-if="isJSDecode" >
      <v-row>
        <v-col col="1">
          <v-select
            label="Script"
            v-model="scriptId"
            :items="decodeScript"
            item-title="name"
            item-value="id"
          ></v-select>
        </v-col>
        <v-col>
          <v-btn @click="addScript">
            {{ $t('page.dashboard.device.decode.add_script') }}
          </v-btn>
          <v-btn @click="deleteScript">
            {{ $t('page.dashboard.device.decode.del_script') }}
          </v-btn>
          <v-btn @click="cloneScript">
            {{ $t('page.dashboard.device.decode.clone_script') }}
          </v-btn>
        </v-col>
      </v-row>
      <div style="height: 400px" class="overflow-scroll">
        <div class="flex">
          <div>data</div>
          <div class="border-blue-400 w-full border-xl">
            <HexInput v-model="hexData" :length="100" class="w-full"/>
          </div>
        </div>
        <DecodeMod v-model="scriptObj" v-if="showCode" class="flex-1" />
        <v-row>
          <DecodeMsg :script="scriptObj" :res="testRes" />
        </v-row>
      </div>
      <v-row class="mt-10">
        <v-btn @click='cancelScript'>
          {{ $t('page.dashboard.device.decode.cancel_script') }}
        </v-btn>
        <v-btn @click='saveScript'>
          {{ $t('page.dashboard.device.decode.save_script') }}
        </v-btn>
        <v-btn @click='setDeviceScript'>
          {{ $t('page.dashboard.device.decode.apply_script') }}
        </v-btn>
        <v-btn @click='testScript'>
          {{ $t('page.dashboard.device.decode.test_script') }}
        </v-btn>
      </v-row>
    </template>
    <template v-else>
      <v-text-field v-model="mapName"></v-text-field>
      <CodeMap v-model="mapData" :data-type="dataTypeArray" />
      <v-btn @click="showCodePage = true">show code</v-btn>
      <v-btn @click="updateMap">update</v-btn>
      <v-dialog max-width="1000" v-model="showCodePage">
        <template v-slot:default>
          <v-card title="Code">
            <v-row>
              <v-checkbox label="Battery" v-model="battery"></v-checkbox>
              <v-checkbox label="Charging" v-model="charging"></v-checkbox>
            </v-row>
            <v-card-text>
              <v-expansion-panels>
                <v-expansion-panel
                  title="sensor_data_packet.h"
                >
                  <v-expansion-panel-text>
                    <CodeEditor v-model="headerFile" cpp disabled />
                  </v-expansion-panel-text>
                </v-expansion-panel>
                <v-expansion-panel
                  title="sensor_data_packet.cpp"
                >
                  <v-expansion-panel-text>
                    <CodeEditor v-model="cppCode" cpp disabled/>
                  </v-expansion-panel-text>
                </v-expansion-panel>
              </v-expansion-panels>
            </v-card-text>

            <v-card-actions>
              <v-spacer></v-spacer>

            </v-card-actions>
          </v-card>
        </template>
      </v-dialog>
    </template>
  </v-card>
</template>

<script setup lang="ts">
import { useRoute } from 'vue-router'
import { computed, onMounted, reactive, ref, watch } from 'vue'
import HexInput from '@/components/widget/HexInput.vue'
import DecodeMod from '@/components/device/DecodeMod.vue'
import DecodeMsg from '@/components/device/DecodeMsg.vue'
import type { DeviceResp, TestScriptResult } from '@/type/response'
import { api } from '@/composables/api'
import { isUndefined } from 'lodash-es'
import {dataTypeArray, headerFile, cppFile} from '@/utils/define'
import { useSuccessMessage, useWarningMessage } from '@/composables/notify'
import CodeMap from '@/components/device/CodeMap.vue'
import CodeEditor from '@/components/widget/CodeEditor.vue'
import { cppcode } from '@/utils/cppcode'

const route = useRoute();
const deviceID = route.params.id as string;
const testRes = ref<TestScriptResult>({

  state: false,
  result: ''
});
const battery = ref(false);
const charging = ref(false);

const deviceInfo = ref<DeviceResp>();
const hexData = ref("");
const mapName = ref("");
const mapData = reactive([]);

const updateDeviceInfo = async () => {
  deviceInfo.value = await api.getDeviceInfo(deviceID);
}

const decodeScript = ref()

const defaultScript = {
  name: "",
  script: `export function decodeUplink(data) {
  return {
    data: [
      { data: data.bytes[0], id: 0 }
    ]
  }
}
`,
  lang: "",
  map: [] as any
};

const cppCode = computed(() => cppcode(battery.value, charging.value, mapData));
const isJSDecode = ref(false)
const showCodePage = ref(false)
const scriptObj = ref(defaultScript)
const scriptId = ref(deviceInfo.value?.script)

const showCode = ref(!!deviceInfo.value?.script);

const updateScript = () => {
  if (isUndefined(decodeScript.value)) {
    return
  }
  for (const item of decodeScript.value) {
    if (item.id === scriptId.value) {
      scriptObj.value = item;
      showCode.value = true;
      break;
    }
  }
}

const getScripts = async () => {
  decodeScript.value = await api.getDecodeScript();
}

onMounted(async () => {
  await updateDeviceInfo();
  await getScripts();
  scriptId.value = deviceInfo.value?.script;
  updateScript();
  await asyncRemoteMap();
})

watch(scriptId, () => {
  updateScript();
})

const addScript = () => {
  scriptObj.value = defaultScript;
  scriptId.value = undefined;
  showCode.value = true;
}


const cancelScript = async () => {
  await api.putDeviceInfo(deviceID,{
    reset_script: true
  })
  scriptId.value = undefined;
  scriptObj.value = defaultScript;
}

const saveScript = async () => {
  let dbScript
  try {
    dbScript = await api.postDecodeScript({
      id: scriptId.value,
      name: scriptObj.value.name,
      script: scriptObj.value.script,
      lang: 'JS',
      map: scriptObj.value.map
    })
  } catch (e) {
    useWarningMessage(e.message)
    return;
  }
  if (!decodeScript.value.find((item) => item.id === dbScript.id)) {
    decodeScript.value.push(dbScript);
  }
  scriptId.value = dbScript.id;
  scriptObj.value = dbScript;
}

const cloneScript = () => {
  scriptId.value = undefined;
  scriptObj.value.name = scriptObj.value.name + "-clone";
}

const deleteScript = async () => {
  if (!isUndefined(scriptId.value)) {
    await api.deleteDecodeScript(scriptId.value);
    await getScripts();
  }
}
const setDeviceScript = async () => {
  await saveScript();
  if (!isUndefined(scriptId.value)) {
    await api.putDecodeScript(deviceID, scriptId.value);
  }
}
const testScript = () => {
  const script = scriptObj.value.script
  if (!script.includes("export")) {
    useWarningMessage("not found export function decodeUplink(data)")
    return
  }
  const preRunScript = script.replace("export", "");
  const data = hexData.value.match(/.{1,2}/g)?.map(item => `0x${item}` ).join(",");
  if (typeof data === "undefined") {
    useWarningMessage("data is null")
    return
  }
  const code = `
    ${preRunScript}
    decodeUplink({ bytes: [ ${data} ] })
  `;
  console.log(code)
  let res
  try {
    res = eval(code)
  } catch (e) {
    useWarningMessage(e.message)
    return;
  }
  useSuccessMessage(JSON.stringify(res))
}

const updateMap = async () => {
  const idSet = new Set();
  for (const item of mapData) {
    item.d_id = parseInt(item.d_id);
    idSet.add(item.d_id);
  }

  if (idSet.size !== mapData.length) {
    useWarningMessage("data id most unique")
    return
  }
  const updateMap = mapData.map(it => {
    return {
      data_id: it.d_id,
      data_name: it.d_name,
      data_unit: it.d_unit,
      data_type: it.d_type,
    }
  });
  await api.postDataMap(deviceID, { name: mapName.value, map: updateMap })
  useSuccessMessage("ok")
}

const asyncRemoteMap = async () => {
  const map = await api.getDataMap(deviceID);
  if (!map) {
    return;
  }
  mapName.value = map.name;
  for (const item of map.map) {
    mapData.push({
      d_id: item.data_id,
      d_name: item.data_name,
      d_unit: item.data_unit,
      d_type: item.data_type,
    });
  }
}
</script>
