<template>
  <nav class="sidebar">
    <button class="close-btn" @click="$emit('close-sidebar')">✖</button>
    <ul>
      <li>
        <router-link to="/" class="nav-link">
          <img :src="homeIcon" class="icon" alt="home" />
          <span>Главная</span>
        </router-link>
      </li>
      <li>
        <router-link to="/about" class="nav-link">
          <img :src="homeIcon" class="icon" alt="home" />
          <span>Товары</span>
        </router-link>
      </li>
      <li>
        <router-link to="/contact" class="nav-link">
          <img :src="homeIcon" class="icon" alt="home" />
          <span>Заказы</span>
        </router-link>
      </li>
      <li>
        <router-link to="/contact" class="nav-link">
          <img :src="homeIcon" class="icon" alt="home" />
          <span>Движения товаров</span>
        </router-link>
      </li>
    </ul>
  </nav>
</template>

<script setup>
import homeIcon from '@/assets/sidebar/homeIcon.svg'
</script>

<style scoped>
.sidebar {
  position: relative;
  width: 200px;
  height: 100%;
  background-color: #fafafa;
  box-sizing: border-box;
  border-right: 1px solid #ebebeb;
  padding: 1rem;
}

.close-btn {
  position: absolute;
  top: 25px;
  right: 1rem;
  background: transparent;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
}

ul {
  list-style: none;
  padding: 0;
  margin-top: 60px;
}

.nav-link {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  text-decoration: none;
  color: black;
  padding: 0.5rem 0;
}

.nav-link:hover {
  background-color: #eaeaea;
  border-radius: 4px;
}

.icon {
  width: 20px;
  height: 20px;
}
</style>
