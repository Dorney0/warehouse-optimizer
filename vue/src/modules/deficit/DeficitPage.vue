<script setup>
import { ref, watch } from 'vue'
import axios from 'axios'
import { API_URL } from '@/api/url'

const responseData = ref(null)
const deficitResults = ref([])
const errorMessage = ref('')

const selectedEndpoint = { path: '/analyze_deficit' }  // фиксированный эндпоинт
const selectedMethod = 'GET'                           // фиксированный метод

// Анализ дефицита: фильтруем элементы с deficit > 0
function analyzeDeficit() {
  if (!responseData.value || !Array.isArray(responseData.value)) {
    deficitResults.value = []
    return
  }
  deficitResults.value = responseData.value.filter(item => item.deficit > 0)
}

// Следим за изменением responseData и анализируем
watch(responseData, (newVal) => {
  if (newVal) {
    analyzeDeficit()
  } else {
    deficitResults.value = []
  }
})

const sendRequest = async () => {
  errorMessage.value = ''
  responseData.value = null
  deficitResults.value = []

  try {
    const res = await axios({
      method: selectedMethod.toLowerCase(),
      url: API_URL + selectedEndpoint.path
    })
    responseData.value = res.data
  } catch (e) {
    errorMessage.value = e.response?.data || e.message
  }
}
</script>

<template>
  <div class="container">
    <button class="send-button" @click="sendRequest">Отправить</button>

    <div class="manual-analysis">
      <h3>Автоматический анализ дефицита</h3>

      <ul v-if="deficitResults.length > 0">
        <li v-for="(item, idx) in deficitResults" :key="idx">
          Необходимо закупить "<strong>{{ item.name }}</strong>" на склад в количестве <strong>{{ item.deficit }}</strong> штук
        </li>
      </ul>

      <p v-else class="no-deficit">Нет дефицита</p>

      <p v-if="errorMessage" class="error">{{ errorMessage }}</p>
    </div>
  </div>
</template>

<style scoped>
.container {
  max-width: 480px;
  margin: 60px auto;
  padding: 30px 35px;
  text-align: center;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  background: #f9f9f9;
  border-radius: 14px;
  box-shadow: 0 0 25px rgba(0,0,0,0.12);
  user-select: none;
}

.send-button {
  padding: 14px 36px;
  font-size: 17px;
  background-color: #4caf50;
  border: none;
  color: white;
  border-radius: 7px;
  cursor: pointer;
  margin-bottom: 38px;
  transition: background-color 0.3s ease;
  box-shadow: 0 4px 8px rgba(76,175,80,0.45);
}

.send-button:hover {
  background-color: #45a049;
}

.manual-analysis h3 {
  margin-bottom: 28px;
  color: #2e7d32;
  font-weight: 700;
}

.manual-analysis ul {
  list-style: none;
  padding: 0;
  margin: 0 auto;
  max-width: 440px;
}

.manual-analysis li {
  background-color: #e8f5e9;
  margin: 12px 0;
  padding: 16px 22px;
  border-radius: 9px;
  font-size: 17px;
  color: #2e7d32;
  box-shadow: 0 2px 6px rgba(46,125,50,0.28);
  text-align: left;
}

.no-deficit {
  font-size: 19px;
  color: #999;
  font-style: italic;
  margin-top: 24px;
}

.error {
  margin-top: 30px;
  color: #d32f2f;
  font-weight: 600;
  font-size: 16px;
}
</style>
