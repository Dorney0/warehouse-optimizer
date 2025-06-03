<script setup>
import { ref, watch } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const props = defineProps({
  jsonRequestBody: String
})
const id = ref(0)
const quantity = ref(0)
const movement_type = ref('')
const entity_id = ref(0)
const related_order_id = ref(null)
const showRelatedOrder = ref(true)
const statusMessage = ref('')
const movementTypes = [
  { value: 'incoming', label: 'входящий' },
  { value: 'outgoing', label: 'исходящий' }
]

const handleSubmit = async () => {
  try {
    const payload = {
      id: id.value,
      quantity: quantity.value,
      movement_time: new Date().toISOString(),
      movement_type: movement_type.value,
      entity_id: entity_id.value,
      related_order_id: related_order_id.value === null ? null : Number(related_order_id.value)
    }
    console.log("movement_type:", JSON.stringify(movement_type.value));
    console.log('Отправляемый payload:', JSON.stringify(payload, null, 2))
    const response = await axios.put(`${API_URL}/stock_movements/`, payload)
    statusMessage.value = '🔄🟡 Запись успешно обновлена!'
    console.log('Ответ сервера:', response.data)
  } catch (error) {
    console.error('Ошибка при создании записи:', error)
    statusMessage.value = '❌ Ошибка при создании записи'
  }
}

const clearRelatedOrder = () => {
  related_order_id.value = null
  showRelatedOrder.value = false
}

const restoreRelatedOrder = () => {
  showRelatedOrder.value = true
  related_order_id.value = 0
}
watch(() => props.jsonRequestBody, (newJson) => {
  try {
    const data = JSON.parse(newJson)
    id.value = data.id ?? null
    quantity.value = data.quantity ?? 0
    movement_type.value = data.movement_type ?? ''
    entity_id.value = data.entity_id ?? 0
    related_order_id.value = data.related_order_id ?? null
    showRelatedOrder.value = related_order_id.value !== null
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
      <label>Количество (quantity):</label>
      <input type="number" v-model.number="quantity" />

      <label>Тип перемещения (movement_type):</label>
      <select v-model="movement_type">
        <option v-for="type in movementTypes" :key="type.value" :value="type.value">
          {{ type.label }}
        </option>
      </select>

      <label>ID сущности (entity_id):</label>
      <input type="number" v-model.number="entity_id" />

      <template v-if="showRelatedOrder">
        <label>ID связанного заказа (related_order_id):</label>
        <div class="related-order-wrapper">
          <input type="number" v-model.number="related_order_id" />
          <button class="icon-button" @click="clearRelatedOrder">❌</button>
        </div>
      </template>

      <template v-else>
        <button class="icon-button add-button" @click="restoreRelatedOrder">+ Добавить связанный заказ</button>
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
  height: 100%; /* или фиксированная высота, например 550px */
}

.create-form {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  width:400px;
}

input,
select {
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
  background-color: #c6f6d5;   /* чуть ярче при наведении */
}
</style>
