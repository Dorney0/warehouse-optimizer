<template>
  <div id="app-layout">
    <AppSidebar
        :class="{ collapsed: !sidebarVisible }"
        @close-sidebar="sidebarVisible = false"
    />
    <div class="main-content" :class="{ expanded: !sidebarVisible }">
      <AppHeader
          :sidebar-visible="sidebarVisible"
          @toggle-sidebar="sidebarVisible = $event"
      />
      <router-view />
    </div>
  </div>
</template>

<script>
import AppHeader from './components/AppHeader.vue'
import AppSidebar from './components/AppSidebar.vue'

export default {
  name: 'App',
  components: {
    AppHeader,
    AppSidebar
  },
  data() {
    return {
      sidebarVisible: true
    }
  }
}
</script>

<style>
body {
  margin: 0;
}

body::before {
  content: '';
  position: fixed;
  top: 0;
  left: 0;
  height: 1px;
  width: 100%;
  background-color: #ebebeb;
  z-index: 9999;
}

#app-layout {
  position: relative;
  height: 100vh;
  overflow: hidden;
}

.main-content {
  position: absolute;
  top: 0;
  left: 200px;
  right: 0;
  height: 100vh;
  display: flex;
  flex-direction: column;
  transition: left 0.3s ease;
  background: #fafafa;
  z-index: 2;
}

.main-content.expanded {
  left: 0;
}
</style>
