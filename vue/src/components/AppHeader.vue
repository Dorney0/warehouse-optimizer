<template>
  <header class="app-header">
    <button
        class="toggle-btn"
        :class="{ invisible: props.sidebarVisible }"
        @click="$emit('toggle-sidebar', true)"
    >
      ☰
    </button>

    <h1 class="title" :class="{ shifted: !sidebarVisible }">
      <span v-if="route.path === '/'">Warehouse-optimizer</span>
      <span v-else-if="route.path === '/products'">Товары</span>
      <span v-else-if="route.path === '/orders'">Заказы</span>
      <span v-else-if="route.path === '/movements'">Движения товаров</span>
      <span v-else>Warehouse-optimizer</span>
    </h1>
  </header>
</template>

<script setup>
import { useRoute } from 'vue-router'

const props = defineProps({
  sidebarVisible: Boolean
})

const route = useRoute()
</script>

<style scoped>
.app-header {
  display: flex;
  align-items: center;
  height: 84px;
  position: relative;
}

.toggle-btn {
  color: #333;
  position: relative;
  background: transparent;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  opacity: 1;
  transition: opacity 0.3s ease;
  margin-right: 1rem;
  width: 40px;
}

.toggle-btn.invisible {
  opacity: 0;
  pointer-events: none;
}

.title {
  margin-top: 34px;
  font-family: 'AGP-RSB', sans-serif;
  font-weight: normal;
  font-size: 2.5rem;
  color: black;
  transition: margin-left 0.3s ease;
}

</style>
