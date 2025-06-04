<template>
  <div class="center-wrapper" v-if="!showTree">
    <button class="show-tree-btn" @click="openTree">Показать дерево</button>
  </div>

  <div v-if="showTree" class="tree-popup" @click.self="showTree = false">
    <div class="tree-container" @click.stop>
      <button class="close-btn" @click="showTree = false">✖</button>
      <svg ref="svg" :width="svgWidth" :height="svgHeight" xmlns="http://www.w3.org/2000/svg"></svg>
    </div>
  </div>
</template>


<script setup>
import { ref, watch, nextTick } from 'vue'
import { fetch_entities_with_children } from './api/api'
import {drawTree} from "@/utils/treeDrawer";

const showTree = ref(false)
const svgWidth = '100%'
const svgHeight = '100%'
const svg = ref(null)
const responseData = ref(null)

watch(showTree, async (visible) => {
  if (visible && responseData.value) {
    await nextTick()
    drawTree(responseData.value, svg.value)
  }
})
async function openTree() {
  const data = await fetch_entities_with_children()
  console.log('Fetched tree data:', JSON.stringify(data, null, 2))

  if (!data || Object.keys(data).length === 0) {
    console.warn('Данные не получены или пусты');
    return
  }

  responseData.value = data[0]
  showTree.value = true
}

</script>

<style scoped>
.center-wrapper {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100vh;
  width: 100vw;
}

.show-tree-btn {
  display: block;
  margin: 40px auto;
  padding: 12px 24px;
  background-color: #007acc;
  color: white;
  font-size: 18px;
  font-weight: bold;
  border: none;
  border-radius: 999px;
  cursor: pointer;
  box-shadow: 0 4px 10px rgba(0, 0, 0, 0.15);
  transition: background-color 0.3s ease, transform 0.2s ease;
}

.show-tree-btn:hover {
  background-color: #005fa3;
  transform: scale(1.05);
}

.show-tree-btn:active {
  transform: scale(0.98);
}

.tree-popup {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  background: rgba(0,0,0,0.5);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 9999;
}
.tree-container {
  background: white;
  padding: 20px;
  border-radius: 8px;
  width:100%;
  height: 100%;
  overflow: auto;
  position: relative;
}
.close-btn {
  position: absolute;
  top: 10px;
  right: 10px;
  border: none;
  background: transparent;
  font-size: 20px;
  cursor: pointer;
}

h4 {
  margin: 0;
}

.endpoint-list li {
  padding: 8px 12px;
  cursor: pointer;
  border-bottom: 1px solid #eee;
}
.endpoint-list li:hover {
  background-color: #f0f0f0;
}
.endpoint-list li.selected {
  background-color: #007acc;
  color: white;
}

.response-content pre {
  text-align: left;
}

.tree-popup {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background-color: rgba(0,0,0,0.4);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 9999;
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
  width:100%;
  height:100%;
  overflow: auto;
}

.tree-container {
  height:100%;
  width:100%;
  background: white;
  border-radius: 8px;
  padding: 20px;
  max-width: 90vw;
  max-height: 90vh;
  box-shadow: 0 0 15px rgba(0,0,0,0.3);
  position: relative;
  overflow: auto;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
}

.close-btn {
  position: absolute;
  top: 5px;
  right: 10px;
  border: none;
  background: none;
  font-size: 20px;
  cursor: pointer;
}

</style>
