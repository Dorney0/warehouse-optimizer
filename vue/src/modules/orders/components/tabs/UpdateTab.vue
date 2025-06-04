<script setup>
import { ref, watch } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const props = defineProps({
  jsonRequestBody: String
})

const id = ref(0)
const order_number = ref('')
const customer_name = ref('')
const total_amount = ref(0)
const status = ref('')
const entity_id = ref(null)
const statusMessage = ref('')

const statusEnum = {
  Pending: 'В ожидании',
  Processing: 'В обработке',
  Completed: 'Завершён'
}

const statusOptions = Object.entries(statusEnum).map(([key, value]) => ({
  value: key,
  label: value
}))

const handleSubmit = async () => {
  try {
    const payload = {
      id: id.value,
      order_number: order_number.value,
      customer_name: customer_name.value,
      total_amount: total_amount.value,
      status: status.value,
      entity_id: entity_id.value === null ? null : Number(entity_id.value)
    }
    console.log('Отправляемый payload:', JSON.stringify(payload, null, 2))
    const response = await axios.put(`${API_URL}/orders/`, payload)
    statusMessage.value = '🔄🟡 Запись успешно обновлена!'
    console.log('Ответ сервера:', response.data)
  } catch (error) {
    console.error('Ошибка при обновлении записи:', error)
    statusMessage.value = '❌ Ошибка при обновлении записи'
  }
}

watch(() => props.jsonRequestBody, (newJson) => {
  try {
    const data = JSON.parse(newJson)
    id.value = data.id ?? 0
    order_number.value = data.order_number ?? ''
    customer_name.value = data.customer_name ?? ''
    total_amount.value = data.total_amount ?? 0
    status.value = data.status ?? ''
    entity_id.value = data.entity_id ?? null
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

      <label>Номер заказа (order_number):</label>
      <input type="text" v-model="order_number" />

      <label>Имя клиента (customer_name):</label>
      <input type="text" v-model="customer_name" />

      <label>Общее количество (total_amount):</label>
      <input type="number" v-model.number="total_amount" step="0.01" />

      <label>Статус (status):</label>
      <select v-model="status">
        <option
            v-for="option in statusOptions"
            :key="option.value"
            :value="option.value"
        >{{ option.label }}</option>
      </select>

      <label>ID сущности (entity_id):</label>
      <input type="int" v-model="entity_id" />

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

input,
select {
  padding: 0.5rem;
  font-size: 1rem;
  border-radius: 4px;
  border: 1px solid #ccc;
  font-family: inherit;
  appearance: none; /* Убирает стандартный стиль стрелки (кроссбраузерно) */
  background-color: white;
  background-image: url("data:image/svg+xml,%3Csvg fill='none' stroke='%23666' stroke-width='2' viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' d='M6 9l6 6 6-6'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1rem 1rem;
}

select {
  cursor: pointer;
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
