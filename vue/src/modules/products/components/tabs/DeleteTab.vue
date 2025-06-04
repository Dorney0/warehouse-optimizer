<script setup>
import { ref, watch } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const props = defineProps({
  jsonRequestBody: String
})

const id = ref(null)
const statusMessage = ref('')

const handleDelete = async () => {
  if (!id.value) {
    statusMessage.value = '❗ Пожалуйста, введите ID для удаления'
    return
  }

  try {
    const response = await axios.delete(`${API_URL}/entities/${id.value}`)
    statusMessage.value = '✅ Запись успешно удалена!'
    console.log('Удалено:', response.data)
  } catch (error) {
    console.error('Ошибка при удалении записи:', error)
    statusMessage.value = '❌ Ошибка при удалении записи'
  }
}

watch(() => props.jsonRequestBody, (newJson) => {
  try {
    const data = JSON.parse(newJson)
    id.value = data.id ?? null
  } catch (e) {
    console.error('Ошибка при разборе JSON:', e)
  }
}, { immediate: true })
</script>

<template>
  <div class="form-wrapper">
    <div class="delete-form">
      <strong>Введите ID или выберите запись из таблицы</strong>
      <label for="id">ID для удаления:</label>
      <input id="id" type="number" v-model.number="id" />
      <button class="delete-button" @click="handleDelete">Удалить запись</button>
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

.delete-form {
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

.delete-button {
  padding: 0.5rem 1rem;
  background-color: #e53e3e;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: bold;
}

.delete-button:hover {
  background-color: #c53030;
}

.status-message {
  font-weight: bold;
  margin-top: 0.5rem;
}
</style>
