<script setup>
import { ref, onMounted, nextTick, onBeforeUnmount, watch, toRefs } from 'vue';

const props = defineProps({
  columns: Array,
  rows: Array
});

const { rows } = toRefs(props);

const tableBody = ref(null);
const customScrollbar = ref(null);
const scrollbarThumb = ref(null);

let dragging = false;
let startY = 0;
let resizeObserver = null;

const containerPadding = 4;

const updateThumb = () => {
  const container = customScrollbar.value;
  const thumb = scrollbarThumb.value;
  if (!container || !thumb || !tableBody.value) return;

  const containerHeight = container.clientHeight;
  const contentHeight = tableBody.value.scrollHeight;
  const visibleHeight = tableBody.value.clientHeight;

  if (contentHeight <= visibleHeight) {
    thumb.style.height = '0px';
    return;
  }

  const trackHeight = containerHeight - 2 * containerPadding;
  const thumbHeight = Math.max((visibleHeight / contentHeight) * trackHeight, 30);
  thumb.style.height = `${thumbHeight}px`;

  const scrollRatio = tableBody.value.scrollTop / (contentHeight - visibleHeight);
  const maxThumbTop = trackHeight - thumbHeight;

  const thumbTop = containerPadding + scrollRatio * maxThumbTop;
  thumb.style.top = `${thumbTop}px`;
};

const onMouseMove = (e) => {
  if (!dragging) return;
  e.preventDefault();

  const container = customScrollbar.value;
  const thumb = scrollbarThumb.value;
  if (!container || !thumb) return;

  const containerRect = container.getBoundingClientRect();
  const trackHeight = container.clientHeight - 2 * containerPadding;

  let newTop = e.clientY - containerRect.top - startY;
  newTop = Math.max(containerPadding, Math.min(newTop, containerPadding + trackHeight - thumb.clientHeight));
  newTop -= containerPadding;

  thumb.style.top = `${containerPadding + newTop}px`;

  const scrollRatio = newTop / (trackHeight - thumb.clientHeight);
  const maxScrollTop = tableBody.value.scrollHeight - tableBody.value.clientHeight;
  tableBody.value.scrollTop = scrollRatio * maxScrollTop;
};

const onMouseUp = () => {
  dragging = false;
  document.body.style.userSelect = '';
  document.removeEventListener('mousemove', onMouseMove);
  document.removeEventListener('mouseup', onMouseUp);
};

onMounted(async () => {
  if (!tableBody.value || !customScrollbar.value || !scrollbarThumb.value) return;

  await nextTick();

  updateThumb();

  tableBody.value.addEventListener('scroll', () => {
    if (!dragging) updateThumb();
  });

  resizeObserver = new ResizeObserver(() => {
    updateThumb();
  });
  resizeObserver.observe(tableBody.value);

  window.addEventListener('resize', updateThumb);

  scrollbarThumb.value.addEventListener('mousedown', (e) => {
    dragging = true;
    const thumbRect = scrollbarThumb.value.getBoundingClientRect();
    startY = e.clientY - thumbRect.top;

    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  });
});

onBeforeUnmount(() => {
  if (resizeObserver) resizeObserver.disconnect();
  window.removeEventListener('resize', updateThumb);
});

watch(rows, () => {
  nextTick(() => updateThumb());
}, { immediate: true, deep: true });
</script>

<template>
    <div class="table-container">
      <div class="table">

        <div class="header-row">
          <div class="cell" v-for="col in columns" :key="col">
            {{ col }}
          </div>
        </div>

        <div class="table-body" ref="tableBody">
          <div
              class="data-row"
              v-for="(row, index) in rows"
              :key="index"
              @click="$emit('row-click', row)"
          >
            <div class="cell" v-for="col in columns" :key="col">
              {{ row[col] }}
            </div>
          </div>
        </div>
      </div>

      <div class="custom-scrollbar" ref="customScrollbar">
        <div class="scrollbar-thumb" ref="scrollbarThumb"></div>
      </div>
    </div>
</template>

<style scoped>
.data-row:hover {
  background-color: #e0f0ff; /* светло-голубой фон при наведении */
  cursor: pointer; /* чтобы курсор менялся на указатель */
}

.table-container {
  display: flex;
  height: 100%;
  gap: 8px;
}

.table {
  flex: 1;
  display: flex;
  flex-direction: column;
  font-family: sans-serif;
  min-width: 600px;
  overflow: hidden;
  border-radius: 12px;
}

.table-body {
  max-height: 550px;
  flex-grow: 1;
  overflow-y: auto;
  -ms-overflow-style: none;  /* IE и Edge */
  scrollbar-width: none;  /* Firefox */
}

.header-row,
.data-row {
  display: flex;
  border-bottom: 1px solid #e0e0e0;
}

.header-row {
  background-color: #f9f9f9;
  font-weight: bold;
}

.cell {
  color: #333;
  flex: 1;
  padding: 0.5rem;
  border-right: 1px solid #ddd;
  word-break: break-word;
}

.cell:last-child {
  border-right: none;
}

.custom-scrollbar {
  position: relative;
  height: auto;
  width: 12px;
  background-color: #eee;
  border-radius: 12px;
  box-sizing: border-box;
  user-select: none;
}

/* Ручка скролла — черный прямоугольник со скруглениями */
.scrollbar-thumb {
  position: absolute;
  left: 2px;
  width: 8px;
  background-color: gray;
  top:4px;
  border-radius: 12px;
  cursor: pointer;
  transition: background-color 0.2s;
}

.scrollbar-thumb:hover {
  background-color: #333;
}
</style>
