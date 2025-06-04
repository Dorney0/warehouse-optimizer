<script setup>
import { ref, watch } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const props = defineProps({
  jsonRequestBody: String
})

const id = ref(0)
const name = ref('')
const quantity = ref(0)
const level = ref(0)
const parent_id = ref(null)
const showParent = ref(true)
const statusMessage = ref('')

const handleSubmit = async () => {
  try {
    const payload = {
      id: id.value,
      name: name.value,
      quantity: quantity.value,
      level: level.value,
      parent_id: parent_id.value === null ? null : Number(parent_id.value)
    }

    console.log('Отправляемый payload:', JSON.stringify(payload, null, 2))

    const response = await axios.put(`${API_URL}/entities/`, payload)
    statusMessage.value = '🔄🟡 Запись успешно обновлена!'
    console.log('Ответ сервера:', response.data)
  } catch (error) {
    console.error('Ошибка при обновлении записи:', error)
    statusMessage.value = '❌ Ошибка при обновлении записи'
  }
}

const clearParent = () => {
  parent_id.value = null
  showParent.value = false
}

const restoreParent = () => {
  showParent.value = true
  parent_id.value = 0
}

watch(() => props.jsonRequestBody, (newJson) => {
  try {
    const data = JSON.parse(newJson)
    id.value = data.id ?? 0
    name.value = data.name ?? ''
    quantity.value = data.quantity ?? 0
    level.value = data.level ?? 0
    parent_id.value = data.parent_id ?? null
    showParent.value = parent_id.value !== null
  } catch (e) {
    console.error('Ошибка при разборе JSON:', e)
  }
}, { immediate: true })
</script>

<template>
  <div class="form-wrapper">
    <div class="create-form">
      <strong>Введите данные или выберите запись из таблицы</strong>

      <label>Идентификатор (id):</label>
      <input type="number" v-model.number="id" />

      <label>Имя (name):</label>
      <input type="text" v-model="name" />

      <label>Количество (quantity):</label>
      <input type="number" v-model.number="quantity" />

      <label>Уровень (level):</label>
      <input type="number" v-model.number="level" />

      <template v-if="showParent">
        <label>ID родителя (parent_id):</label>
        <div class="related-order-wrapper">
          <input type="number" v-model.number="parent_id" />
          <button class="icon-button" @click="clearParent">❌</button>
        </div>
      </template>

      <template v-else>
        <button class="icon-button add-button" @click="restoreParent">+ Добавить родителя</button>
      </template>

      <button class="submit-button" @click="handleSubmit">Обновить запись</button>
      <p v-if="statusMessage" class="status-message">{{ statusMessage }}</p>
    </div>
  </div>
</template>

<style scoped>
.form-wrapper {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
}

.create-form {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  width: 400px;
}

input {
  padding: 0.5rem;
  font-size: 1rem;
  border-radius: 4px;
  border: 1px solid #ccc;
}

.related-order-wrapper {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.icon-button {
  background: none;
  border: none;
  font-size: 1.2rem;
  cursor: pointer;
  padding: 0.2rem 0.4rem;
}

.submit-button {
  padding: 0.5rem 1rem;
  background-color: #fca130;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: bold;
}

.submit-button:hover {
  background-color: #d58727;
}

.status-message {
  font-weight: bold;
  margin-top: 0.5rem;
}

.add-button {
  color: #2f855a;
  background-color: #e6ffed;
  border: 1px solid #38a169;
  border-radius: 4px;
  padding: 0.3rem 0.6rem;
  font-weight: 600;
  cursor: pointer;
  transition: background-color 0.2s;
}

.add-button:hover {
  background-color: #c6f6d5;
}
</style>
