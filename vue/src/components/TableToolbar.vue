<template>
  <div class="table-toolbar">
    <div class="left-buttons">
      <button
          v-if="showViewButton"
          class="btn view"
          :class="{ active: activeMode === 'view' }"
          @click="$emit('view')"
      >
        Просмотр
      </button>
      <button
          v-if="showCreateButton"
          class="btn create"
          :class="{ active: activeMode === 'create' }"
          @click="$emit('create')"
      >
        Создание
      </button>
      <button
          v-if="showUpdateButton"
          class="btn update"
          :class="{ active: activeMode === 'update' }"
          @click="$emit('update')"
      >
        Обновление
      </button>
      <button
          v-if="showDeleteButton"
          class="btn delete"
          :class="{ active: activeMode === 'delete' }"
          @click="$emit('delete')"
      >
        Удаление
      </button>
    </div>

    <div class="right-controls">
      <input type="date" :value="props.dateFrom" @input="onDateFromChange" class="date-input" />
      <span>–</span>
      <input type="date" :value="props.dateTo" @input="onDateToChange" class="date-input" />

      <button
          v-if="showRefreshButton"
          class="btn up"
          @click="$emit('up')"
      >
        <img src="@/assets/update.svg" alt="Обновить" class="icon" />
      </button>
    </div>
  </div>
</template>

<script setup>
const props = defineProps({
  activeMode: String,
  showViewButton: { type: Boolean, default: true },
  showCreateButton: { type: Boolean, default: true },
  showUpdateButton: { type: Boolean, default: true },
  showDeleteButton: { type: Boolean, default: true },
  showRefreshButton: { type: Boolean, default: true },
  dateFrom: String,
  dateTo: String
})

const emit = defineEmits(['view', 'create', 'update', 'delete', 'up', 'update:dateFrom', 'update:dateTo'])

const onDateFromChange = (e) => emit('update:dateFrom', e.target.value)
const onDateToChange = (e) => emit('update:dateTo', e.target.value)
</script>

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
.btn.active {
  transform: scale(1.05);
  opacity: 0.9;
}

.btn.view.active {
  background-color: #2b8ed4;
}
.btn.create.active {
  background-color: #3aa877;
}
.btn.update.active {
  background-color: #e38e1a;
}
.btn.delete.active {
  background-color: #d23232;
}

.right-controls {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.date-input {
  padding: 0.3rem 0.6rem;
  border: 1px solid #ccc;
  border-radius: 6px;
  font-size: 14px;
}
</style>

