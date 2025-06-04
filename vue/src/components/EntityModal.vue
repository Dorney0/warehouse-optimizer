<template>
  <div class="modal-overlay" @click.self="closeModal">
    <div class="modal-content">
      <h3>Информация о записи</h3>

      <div class="entity-details">
        <div class="detail-row" v-for="(value, key) in entity" :key="key">
          <span class="detail-key">{{ key }}:</span>
          <span class="detail-value">{{ value }}</span>
        </div>
      </div>

      <div class="modal-buttons">
        <button class="modal-button" @click="closeModal">Закрыть</button>
        <button
            v-if="showEditButton"
            class="modal-button edit"
            @click="onEdit">
          Редактировать
        </button>
        <button
            v-if="showDeleteButton"
            class="modal-button del"
            @click="onDelete">
          Удалить
        </button>
      </div>

    </div>
  </div>
</template>

<script setup>
defineProps({
  entity: Object,
  showEditButton: {
    type: Boolean,
    default: true
  },
  showDeleteButton: {
    type: Boolean,
    default: true
  }
})

const emit = defineEmits(['close', 'edit', 'delete'])

const closeModal = () => emit('close')
const onEdit = () => emit('edit')
const onDelete = () => emit('delete')
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 9999;
}

.modal-content {
  background: #fff;
  padding: 24px;
  border-radius: 12px;
  max-width: 500px;
  width: 90%;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
  font-family: sans-serif;
}

.entity-details {
  margin-top: 16px;
  margin-bottom: 20px;
}

.detail-row {
  display: flex;
  justify-content: space-between;
  padding: 6px 0;
  border-bottom: 1px solid #eee;
}

.detail-key {
  font-weight: bold;
  color: #333;
}

.detail-value {
  color: #555;
  max-width: 65%;
  text-align: right;
  word-break: break-word;
}

.modal-buttons {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

.modal-button {
  padding: 8px 14px;
  border: none;
  border-radius: 6px;
  background-color: #1976d2;
  color: white;
  font-size: 14px;
  cursor: pointer;
  transition: background 0.3s;
}

.modal-button:hover {
  background-color: #155a9c;
}

.modal-button.edit {
  background-color: #6c757d;
}

.modal-button.edit:hover {
  background-color: #5a6268;
}

.modal-button.del {
  background-color: #d90f0f;
}

.modal-button.del:hover {
  background-color: #9f1111;
}
</style>
