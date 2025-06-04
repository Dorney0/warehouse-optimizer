<script setup>
import { ref } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const name = ref('')
const quantity = ref(0)
const level = ref(0)
const parent_id = ref(null)
const statusMessage = ref('')

const showParent = ref(true)

const handleSubmit = async () => {
  try {
    const payload = {
      name: name.value,
      quantity: quantity.value,
      level: level.value,
      parent_id: parent_id.value === null ? null : Number(parent_id.value)
    }

    console.log('Отправляемый payload:', payload)
    const response = await axios.post(`${API_URL}/entities/`, payload)
    statusMessage.value = '✅ Сущность успешно создана!'
    console.log('Ответ сервера:', response.data)
  } catch (error) {
    console.error('Ошибка при создании сущности:', error)
    statusMessage.value = '❌ Ошибка при создании сущности'
  }
}

const clearRelatedOrder = () => {
  parent_id.value = null
  showParent.value = false
}

const restoreRelatedOrder = () => {
  showParent.value = true
  parent_id.value = 0
}
</script>


<template>
  <div class="form-wrapper">
    <div class="create-form">
      <strong>Введите данные сущности</strong>

      <label>Имя (name):</label>
      <input type="text" v-model="name" />

      <label>Количество (quantity):</label>
      <input type="number" v-model.number="quantity" />

      <label>Уровень (level):</label>
      <input type="number" v-model.number="level" />

      <template v-if="showParent">
        <label>ID родителя (related_order_id):</label>
        <div class="related-order-wrapper">
          <input type="number" v-model.number="parent_id" />
          <button class="icon-button" @click="clearRelatedOrder">❌</button>
        </div>
      </template>

      <template v-else>
        <button class="icon-button add-button" @click="restoreRelatedOrder">+ Добавить родителя</button>
      </template>

      <button class="submit-button" @click="handleSubmit">Создать сущность</button>
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
  width:400px;
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

.icon-button {
  background: none;
  border: none;
  font-size: 1.2rem;
  cursor: pointer;
  padding: 0.2rem 0.4rem;
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
  color: #2f855a;              /* тёмно-зелёный текст */
  background-color: #e6ffed;   /* светло-зелёный фон */
  border: 1px solid #38a169;   /* основной зелёный */
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
