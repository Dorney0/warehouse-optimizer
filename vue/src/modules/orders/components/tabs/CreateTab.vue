<script setup>
import { ref } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const order_number = ref('')
const customer_name = ref('')
const total_amount = ref(0)
const status = ref('Pending')
const entity_id = ref(0)
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
      order_number: order_number.value,
      customer_name: customer_name.value,
      total_amount: total_amount.value,
      status: status.value,  // отправляем английское значение
      entity_id: entity_id.value === null ? null : Number(entity_id.value)
    }
    console.log('Запрос отправляется на URL:', `${API_URL}/orders/`)

    console.log('Отправляемый payload:', payload)
    const response = await axios.post(`${API_URL}/orders/`, payload)
    statusMessage.value = '✅ Заказ успешно создан!'
    console.log('Ответ сервера:', response.data)
  } catch (error) {
    console.error('Ошибка при создании заказа:', error)
    statusMessage.value = '❌ Ошибка при создании заказа'
  }
}
</script>

<template>
  <div class="form-wrapper">
    <div class="create-form">
      <strong>Введите данные заказа</strong>

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
      <input type="number" v-model.number="entity_id" />

      <button class="submit-button" @click="handleSubmit">Создать заказ</button>
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
