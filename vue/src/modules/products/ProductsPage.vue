<script setup>
import { ref, onMounted, computed } from 'vue'
import { fetchEntities } from './api/api.js'
import FetchTable from '@/components/FetchTable.vue'
import EntityModal from "@/components/EntityModal.vue";
import TableToolbar from "@/components/TableToolbar.vue";
import CreateTab from "./components/tabs/CreateTab.vue"
import UpdateTab from "./components/tabs/UpdateTab.vue"
import DeleteTab from "./components/tabs/DeleteTab.vue"
import TableWrapper from "@/components/TableWrapper.vue";

const entities = ref([])
const activeMode = ref('view')
const jsonRequestBody = ref('{}')  // или пример объекта как строку JSON
const selectedEntity = ref(null)   // Выбранная запись для модалки

const onRowClick = (row) => {
  selectedEntity.value = row
}

const closeModal = () => {
  selectedEntity.value = null
}

onMounted(async () => {
  activeMode.value = 'view'
  try {
    entities.value = await fetchEntities()
  } catch (error) {
    console.error('Ошибка при загрузке:', error)
  }
})

const columns = computed(() => {
  if (entities.value.length === 0) return []
  return Object.keys(entities.value[0])
})

function generateExampleFromEntity(entity) {
  const example = {}
  for (const key in entity) {
    const value = entity[key]
    if (typeof value === 'number') {
      example[key] = 0
    } else if (typeof value === 'string') {
      example[key] = ""
    } else if (typeof value === 'boolean') {
      example[key] = false
    } else if (Array.isArray(value)) {
      example[key] = []
    } else if (typeof value === 'object' && value !== null) {
      example[key] = {}
    } else {
      example[key] = null
    }
  }
  return example
}

const onCreate = () => {
  console.log('Создание записи')
  activeMode.value = 'create'
  if (entities.value.length > 0) {
    const example = generateExampleFromEntity(entities.value[0])
    jsonRequestBody.value = JSON.stringify(example, null, 2)
  }
}

const onView = async () => {
  await onRefresh()
  activeMode.value = 'view'
}

const onUpdate = () => activeMode.value = 'update'
const onDelete = () => activeMode.value = 'delete'

const handleEdit = () => {
  jsonRequestBody.value = JSON.stringify(selectedEntity.value, null, 2)
  activeMode.value = 'update'
  closeModal()
}
const handleDel = () => {
  jsonRequestBody.value = JSON.stringify(selectedEntity.value, null, 2)
  activeMode.value = 'delete'
  closeModal()
}

const onRefresh = async () => {
  activeMode.value = 'up'
  try {
    entities.value = await fetchEntities()
  } catch (error) {
    console.error('Ошибка при обновлении данных:', error)
  } finally {
    activeMode.value = 'view'
  }
}
</script>

<template>
  <TableToolbar :active-mode="activeMode" @view="onView" @create="onCreate" @update="onUpdate" @delete="onDelete"   @up="onRefresh"/>

  <TableWrapper>
    <FetchTable
        v-if="activeMode === 'view'"
        :columns="columns"
        :rows="entities"
        @row-click="onRowClick"
    />
    <CreateTab
        v-if="activeMode === 'create'"
        v-model:jsonRequestBody="jsonRequestBody"
    />
    <UpdateTab
        v-if="activeMode === 'update'"
        v-model:jsonRequestBody="jsonRequestBody"
    />
    <DeleteTab
        v-if="activeMode === 'delete'"
        v-model:jsonRequestBody="jsonRequestBody"
    />
  </TableWrapper>

  <EntityModal
      v-if="selectedEntity"
      :entity="selectedEntity"
      @close="closeModal"
      @edit="handleEdit"
      @delete="handleDel"
  />


</template>
