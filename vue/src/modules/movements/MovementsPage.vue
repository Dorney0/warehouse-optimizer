<script setup>
import { ref, onMounted, computed } from 'vue'
import { fetchMovements } from './api/api.js'
import FetchTable from "@/components/FetchTable.vue"
import TableToolbar from "@/components/TableToolbar.vue"
import TableWrapper from "@/components/TableWrapper.vue"
import CreateTab from "./components/tabs/CreateTab.vue"
import UpdateTab from "./components/tabs/UpdateTab.vue"
import DeleteTab from "./components/tabs/DeleteTab.vue"

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
    entities.value = await fetchMovements()
  } catch (error) {
    console.error('Ошибка при загрузке:', error)
  }
})

const columns = computed(() => {
  if (entities.value.length === 0) return []
  return Object.keys(entities.value[0])
})

// Пример шаблона запроса
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
    entities.value = await fetchMovements()
  } catch (error) {
    console.error('Ошибка при обновлении данных:', error)
  } finally {
    activeMode.value = 'view' // возвращаем обратно в режим просмотра
  }
}

</script>




<template>
  <TableToolbar @view="onView" @create="onCreate" @update="onUpdate" @delete="onDelete"   @up="onRefresh"/>

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

  <!-- Модальное окно -->
  <div v-if="selectedEntity" class="modal-overlay" @click.self="closeModal">
    <div class="modal-content">
      <h3>Информация о записи</h3>

      <div class="entity-details">
        <div class="detail-row" v-for="(value, key) in selectedEntity" :key="key">
          <span class="detail-key">{{ key }}:</span>
          <span class="detail-value">{{ value }}</span>
        </div>
      </div>

      <div class="modal-buttons">
        <button class="modal-button" @click="closeModal">Закрыть</button>
        <button
            class="modal-button edit"
            @click="handleEdit"
        >
          Редактировать
        </button>
        <button
            class="modal-button del"
            @click="handleDel"
        >
          Удалить
        </button>
      </div>
    </div>
  </div>

</template>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 9999;
}

.modal-content {
  background: #fff;
  padding: 24px;
  border-radius: 12px;
  max-width: 500px;
  width: 90%;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
  font-family: sans-serif;
}

.entity-details {
  margin-top: 16px;
  margin-bottom: 20px;
}

.detail-row {
  display: flex;
  justify-content: space-between;
  padding: 6px 0;
  border-bottom: 1px solid #eee;
}

.detail-key {
  font-weight: bold;
  color: #333;
}

.detail-value {
  color: #555;
  max-width: 65%;
  text-align: right;
  word-break: break-word;
}

.modal-buttons {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

.modal-button {
  padding: 8px 14px;
  border: none;
  border-radius: 6px;
  background-color: #1976d2;
  color: white;
  font-size: 14px;
  cursor: pointer;
  transition: background 0.3s;
}

.modal-button:hover {
  background-color: #155a9c;
}

.modal-button.edit {
  background-color: #6c757d;
}

.modal-button.edit:hover {
  background-color: #5a6268;
}

.modal-button.del {
  background-color: #d90f0f;
}

.modal-button.del:hover {
  background-color: #9f1111;
}

</style>