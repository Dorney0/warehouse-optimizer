<template>
  <header class="app-header">
    <button
        class="toggle-btn"
        :class="{ invisible: sidebarVisible }"
        @click="$emit('toggle-sidebar', true)"
    >
      ☰
    </button>
    <h1 class="title" :class="{ shifted: !sidebarVisible }">Warehouse-optimizer</h1>
  </header>
</template>



<script>
export default {
  name: 'AppHeader',
  props: {
    sidebarVisible: Boolean
  }
}
</script>

<style scoped>
.app-header {
  display: flex;
  align-items: center;
  height: 84px;
  position: relative;
}

.toggle-btn {
  position: relative;
  background: transparent;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  opacity: 1;
  transition: opacity 0.3s ease;
  margin-right: 1rem;
  width: 40px; /* зарезервировать место */
}

/* Кнопка скрывается, но сохраняет ширину */
.toggle-btn.invisible {
  opacity: 0;
  pointer-events: none;
}

/* Заголовок — обычное положение */
.title {
  font-size: 1.2rem;
  color: black;
  margin-left: 0;
  transition: margin-left 0.3s ease;
}

</style>
