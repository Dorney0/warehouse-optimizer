<script setup>
import { ref, onMounted, computed } from 'vue'
import {fetchMovements} from './api/api.js'
import FetchTable from '@/components/FetchTable.vue'

const entities = ref([])

onMounted(async () => {
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
</script>

<template>
  <FetchTable :columns="columns" :rows="entities" />
</template>
