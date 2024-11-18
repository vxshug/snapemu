<template>
  <div>
    <v-container>
      <div class="mr-auto ml-auto" style="width: 1230px; height: 50px">
        <div class="flex justify-end">
          <div style="width: 200px;" class="mr-2">
            <v-text-field
              v-model="search"
              :clearable="true"
              variant="outlined"
              prepend-inner-icon="mdi-magnify"
              density="compact"
              rounded="xl"
              label="Search"
              :single-line="true"
              hide-details
            ></v-text-field>
          </div>
          <CreateMqtt @close="closeDialog" />
        </div>
      </div>
      <v-card class="mr-auto ml-auto" width="1230" elevation="6" height="800" rounded="xl">
        <v-col>
          <v-table>
            <thead>
              <tr>
                <th class="text-left">
                  {{$t("page.dashboard.integration.name")}}
                </th>
                <th class="text-left">
                  {{$t("page.dashboard.integration.username")}}
                </th>
                <th class="text-left">
                  {{$t("page.dashboard.integration.password")}}
                </th>
                <th class="text-left">
                  {{$t("page.dashboard.integration.client_id")}}
                </th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="item in tokensComp"
                :key="item.name"
              >
                <td>{{ item.name }}</td>
                <td>
                  <TextClipboard
                    :text="item.username"
                    :password="false"
                  />
                </td>
                <td>
                  <TextClipboard
                    :text="item.password"
                    :password="true"
                  />
                </td>
                <td>
                  <TextClipboard
                    :text="item.client_id"
                    :password="false"
                  />
                </td>
             </tr>
            </tbody>
          </v-table>
        </v-col>
      </v-card>
    </v-container>
  </div>
</template>

<script setup lang="ts">
import { useT } from '@/composables/i18n'
import { computed, onMounted, ref } from 'vue'
import type { IntegrationResp } from '@/type/response'
import { api } from '@/composables/api'
import TextClipboard from '@/components/widget/TextClipboard.vue'
import CreateMqtt from '@/views/dashboard/integration/CreateMqtt.vue'

const t = useT()
const tokens = ref<IntegrationResp>({
  count: 0, tokens: []
});
const search = ref('');

const tokensComp = computed(() => tokens.value.tokens.filter(item => search.value === "" ?  true : item.name.startsWith(search.value)));

const updateToken = async () => {
  tokens.value = await api.getIntegration();
}

const closeDialog = async () => {
  await updateToken();
}

onMounted(async () => {
  await updateToken();
})

const onClickRow = () => {

}

</script>
