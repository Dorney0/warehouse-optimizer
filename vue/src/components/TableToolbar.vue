<template>
  <div class="table-toolbar">
    <div class="left-buttons">
      <button class="btn view" @click="$emit('view')">Просмотр</button>
      <button class="btn create" @click="$emit('create')">Создание</button>
      <button class="btn update" @click="$emit('update')">Обновление</button>
      <button class="btn delete" @click="$emit('delete')">Удаление</button>
    </div>
    <button class="btn up" @click="$emit('up')">
      <img src="@/assets/update.svg" alt="Обновить" class="icon" />
    </button>
  </div>
</template>


<style scoped>
.table-toolbar {
  margin: 2rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.4rem;
  background-color: #fff;
  border: 1px solid #ebebeb;
  border-radius: 12px 12px 0 0;
  box-sizing: border-box;
}

.left-buttons {
  display: flex;
  gap: 0.5rem;
}

.btn {
  padding: 0.3rem 0.8rem;
  border: none;
  border-radius: 6px;
  color: white;
  font-weight: bold;
  cursor: pointer;
}

.btn.view {
  background-color: #61affe;
}
.btn.create {
  background-color: #49cc90;
}
.btn.update {
  background-color: #fca130;
}
.btn.delete {
  background-color: #f93e3e;
}
.btn.up {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 0.4rem;
}

.icon {
  width: 20px;
  height: 20px;
}
</style>

