<template>
  <div class="bigbox">
    <v-container>
      <div class="mr-auto ml-auto" style="width: 1230px; height: 50px">
        <div class="flex justify-end">
          <div style="width: 200px;" class="mr-2">
            <v-text-field
              v-model="search"
              :clearable="true"
              variant="solo-inverted" 
              prepend-inner-icon="mdi-magnify"
              density="compact"
              label="Search"
              :single-line="true"
              hide-details
            ></v-text-field>
          </div>
          <DeviceGroupNew />
        </div>
      </div>
      <v-card class="mr-auto ml-auto" width="1230" elevation="6" height="800" rounded="xl">
        <v-col>
          <v-table>
            <thead>
              <tr>
                <th class="text-left">
                  {{$t("page.dashboard.group.name")}}
                </th>
                <th class="text-left">
                  {{$t("page.dashboard.group.description")}}
                </th>
                <th class="text-left">
                  {{$t("page.dashboard.group.device_count")}}
                </th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="item in groups"
                :key="item.id"
                class="border-4 border-transparent row-line-bg rounded-xl"
                @click="toGroup(item.id)"
              >
                <td>
                  <span>{{ item.name }}</span>
                </td>
                <td>
                  {{ item.description }}
                </td>
                <td>
                  {{ item.device_count }}
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
import { computed, onMounted, ref } from 'vue'
import type { DeviceGroup } from '@/type/response'
import { api } from '@/composables/api'
import DeviceGroupNew from '@/views/dashboard/group/DeviceGroupNew.vue'
import { useRouter } from 'vue-router'

const search = ref('');
const groups = ref<DeviceGroup[]>([]);
const groupCount = computed(() => groups.value.length)
const router = useRouter();

onMounted(async () => {
  groups.value = await api.getGroups();
})

const toGroup = (id: string) => {
  router.push(`/dashboard/group/${id}`)
}

</script>

<style scoped lang="scss">
.row-line-bg {
  background-image: linear-gradient(270deg, #209390 0%, #0D5F5D 100%, rgba(255, 255, 255, 0) 100%);
}
</style>/