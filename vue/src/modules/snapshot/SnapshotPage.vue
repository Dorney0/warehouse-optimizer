<script setup>
import { ref, onMounted, computed } from 'vue'
import { fetchEntities } from './api/api.js'
import FetchTable from '@/components/FetchTable.vue'
import EntityModal from "@/components/EntityModal.vue";
import TableToolbar from "@/components/TableToolbar.vue";
import TableWrapper from "@/components/TableWrapper.vue";

const entities = ref([])
const activeMode = ref('view')
const selectedEntity = ref(null)

const dateFrom = ref('')
const dateTo = ref('')
const onRowClick = (row) => {
  selectedEntity.value = row
}

const closeModal = () => {
  selectedEntity.value = null
}

const columns = computed(() => {
  if (entities.value.length === 0) return []
  return Object.keys(entities.value[0])
})

const filteredEntities = computed(() => {
  if (!dateFrom.value && !dateTo.value) return entities.value

  return entities.value.filter(item => {
    const date = new Date(item.date) // предположим, у тебя есть поле item.date
    const from = dateFrom.value ? new Date(dateFrom.value) : null
    const to = dateTo.value ? new Date(dateTo.value) : null

    return (!from || date >= from) && (!to || date <= to)
  })
})


onMounted(async () => {
  activeMode.value = 'view'
  try {
    entities.value = await fetchEntities(dateFrom.value, dateTo.value)
  } catch (error) {
    console.error('Ошибка при загрузке:', error)
  }
})

const onRefresh = async () => {
  activeMode.value = 'up'
  try {
    entities.value = await fetchEntities(dateFrom.value, dateTo.value)
  } catch (error) {
    console.error('Ошибка при обновлении данных:', error)
  } finally {
    activeMode.value = 'view'
  }
}
</script>

<template>
  <TableToolbar
      :active-mode="activeMode"
      :show-view-button="true"
      :show-create-button="false"
      :show-update-button="false"
      :show-delete-button="false"
      :show-refresh-button="true"
      :date-from="dateFrom"
      :date-to="dateTo"
      @update:dateFrom="dateFrom = $event"
      @update:dateTo="dateTo = $event"
      @up="onRefresh"
  />

  <TableWrapper>
    <FetchTable
        v-if="activeMode === 'view'"
        :columns="columns"
        :rows="filteredEntities"
        @row-click="onRowClick"
    />
  </TableWrapper>

  <EntityModal
      v-if="selectedEntity"
      :entity="selectedEntity"
      :show-edit-button="false"
      :show-delete-button="false"
      @close="closeModal"
  />



</template>
