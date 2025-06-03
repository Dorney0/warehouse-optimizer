<script setup>
import { ref } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const id = ref(0)
const name = ref('')
const quantity = ref(0)
const level = ref(0)
const parent_id = ref(0)

const statusMessage = ref('')

const handleSubmit = async () => {
  try {
    const payload = {
      id: id.value,
      name: name.value,
      quantity: quantity.value,
      level: level.value,
      parent_id: parent_id.value
    }

    const response = await axios.post(`${API_URL}/stock_movements/`, payload)
    statusMessage.value = '✅ Запись успешно создана!'
    console.log('Ответ сервера:', response.data)
  } catch (error) {
    console.error('Ошибка при создании записи:', error)
    statusMessage.value = '❌ Ошибка при создании записи'
  }
}
</script>

<template>
  <div class="create-form">
    <label>ID:</label>
    <input type="number" v-model.number="id" />

    <label>Название (name):</label>
    <input type="text" v-model="name" />

    <label>Количество (quantity):</label>
    <input type="number" v-model.number="quantity" />

    <label>Уровень (level):</label>
    <input type="number" v-model.number="level" />

    <label>Родительский ID (parent_id):</label>
    <input type="number" v-model.number="parent_id" />

    <button class="submit-button" @click="handleSubmit">Создать запись</button>
    <p v-if="statusMessage" class="status-message">{{ statusMessage }}</p>
  </div>
</template>

<style scoped>
.create-form {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  max-width: 400px;
}

input {
  padding: 0.5rem;
  font-size: 1rem;
  border-radius: 4px;
  border: 1px solid #ccc;
}

.submit-button {
  padding: 0.5rem 1rem;
  background-color: #38a169;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: bold;
}

.submit-button:hover {
  background-color: #2f855a;
}

.status-message {
  font-weight: bold;
  margin-top: 0.5rem;
}
</style>
