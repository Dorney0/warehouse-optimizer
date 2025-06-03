<script setup>
</script>

<template>
  <div class="table-wrapper">
    <slot />
  </div>
</template>

<style scoped>
.table-wrapper {
  margin-left: 2rem;
  margin-right: 2rem;
  margin-top: 2rem;
  border: 1px solid #ebebeb;
  border-radius: 12px;
  background-color: #fff;
  max-height: 550px;
  height: 550px;
  overflow: hidden;
  padding: 0.4rem;
  box-sizing: border-box;
}
</style>
