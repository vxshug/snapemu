<template>
  <v-card>
    <v-btn @click="addItem">
      {{ $t('page.dashboard.device.decode.add_map') }}
    </v-btn>
    <v-table>
      <thead>
        <tr>
          <th class="text-left">
            {{ $t('page.dashboard.device.decode.data_name')}}
          </th>
          <th class="text-left">
            {{ $t('page.dashboard.device.decode.data_unit')}}
          </th>
          <th class="text-left">
            {{ $t('page.dashboard.device.decode.data_type')}}
          </th>
          <th class="text-left">
            {{ $t('page.dashboard.device.decode.data_id')}}
          </th>
          <th class="text-left">
            {{ $t('page.dashboard.device.delete')}}
          </th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="(item, index) in model"
          :key="index"
        >
          <td>
            <v-text-field v-model="item.d_name" variant="outlined"></v-text-field>
          </td>
          <td>
            <v-text-field v-model="item.d_unit" variant="outlined"></v-text-field>
          </td>
          <td>
            <v-select
              v-model="item.d_type"
              :items="props.dataType"
            ></v-select>
          </td>
          <td>
            <v-text-field v-model="item.d_id" variant="outlined" type="number"></v-text-field>
          </td>
          <td>
            <v-btn icon="mdi-delete" @click="deleteItem(index)">
            </v-btn>
          </td>
        </tr>
      </tbody>
    </v-table>
  </v-card>
</template>

<script setup lang="ts">

const props = withDefaults(defineProps<{
  dataType: string[]
}>(), {
  dataType: () => ['I32', 'F64', 'Bool']
})

const model = defineModel<{
  d_name: string,
  d_unit: string,
  d_type: string,
  d_id: number,
}[]>({ required: true });

const deleteItem = (index: number) => {
  model.value.splice(index, 1);
}

const addItem = () => {
  model.value.push({
    d_name: "",
    d_type: 'I32',
    d_unit: "",
    d_id: model.value.length,
  });
}

</script>

<style lang="scss" scoped>
</style>
