<template>
  <div>
    <img alt="Vue logo" src="./assets/logo.png" />
    <StartComponent />
  </div>
</template>

<script>
import StartComponent from './components/StartComponent.vue'

export default {
  name: 'App',
  components: {
    StartComponent
  }
}
</script>


<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
