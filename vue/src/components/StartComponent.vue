<template>
  <div class="container">
    <div class="flex">
      <!-- Список эндпоинтов -->
      <ul class="endpoint-list">
        <li
            v-for="(ep, i) in endpoints"
            :key="i"
            :class="{ selected: selectedIndex === i }"
            @click="selectEndpoint(i)"
        >
          {{ ep.path }}
        </li>
      </ul>

      <div class="endpoint-details" v-if="selectedEndpoint">
        <h3>Детали эндпоинта</h3>
        <p><strong>Путь:</strong> {{ selectedEndpoint.path }}</p>
        <p><strong>Методы HTTP:</strong> {{ selectedEndpoint.methods.join(', ') }}</p>

        <div>
          <label for="http-method">Выберите HTTP метод:</label>
          <select v-model="selectedMethod" id="http-method" @change="resetParams">
            <option v-for="m in selectedEndpoint.methods" :key="m" :value="m">{{ m }}</option>
          </select>
        </div>

        <div class="right-side-blocks">
          <h4>Параметры</h4>
          <div class="data-block">
            <div
                v-if="selectedMethod === 'POST' || selectedMethod === 'PUT'"
                style="display: flex; flex-direction: column; height: 100%;"
            >
              <label for="json-body" style="margin-bottom: 8px;">Тело запроса (JSON):</label>
              <textarea
                  id="json-body"
                  v-model="jsonRequestBody"
                  style="flex-grow: 1; width: 100%; font-family: monospace; resize: none;"
              ></textarea>
            </div>

            <div v-else-if="parameterList.length">
              <div v-for="param in parameterList" :key="param.name" class="param-item">
                <label :for="param.name">{{ param.name }} <small>({{ param.in }})</small>:</label>
                <input
                    type="text"
                    :id="param.name"
                    v-model="params[param.name]"
                    :placeholder="param.description || ''"
                />
              </div>
            </div>
            <div v-else>
              <p>Параметры отсутствуют</p>
            </div>
            <div style="margin-top: 10px;">
              <button @click="sendRequest">Отправить</button>
              <button @click="clearResponse">Отмена</button>
            </div>
          </div>
          <h4>Ответ</h4>
          <div class="response-block">
            <div v-if="responseData" class="response-content">
              <pre>{{ formattedResponse }}</pre>
            </div>
            <div v-else>
              <p>Ответ отсутствует</p>
            </div>

            <div v-if="errorMessage" class="error-message">
              <h4>Ошибка:</h4>
              <pre>{{ errorMessage }}</pre>
            </div>
          </div>
        </div>
      </div>

      <div v-else class="endpoint-details">
        <p>Выберите метод слева</p>
      </div>

      <div v-if="responseData && selectedEndpoint.path.startsWith('/analyze_deficit')" class="manual-analysis">
        <h3>Автоматический анализ</h3>

        <div v-if="deficitResults.length > 0">
          <ul>
            <li v-for="(item, idx) in deficitResults" :key="idx">
              Необходимо закупить "{{ item.name }}" на склад в количестве {{ item.deficit }} штук
            </li>
          </ul>
        </div>

        <div v-else>
          <p>Нет дефицита</p>
        </div>
      </div>
      <button v-if="responseData && selectedEndpoint.path.startsWith('/entities_with_children')" @click="showTree = !showTree">
        {{ showTree ? 'Скрыть структуру' : 'Показать структуру' }}
      </button>

      <div v-if="showTree" class="tree-popup" @click.self="showTree = false">
        <div class="tree-container" @click.stop>
          <button class="close-btn" @click="showTree = false">✖</button>
          <svg ref="svg" :width="svgWidth" :height="svgHeight" xmlns="http://www.w3.org/2000/svg"></svg>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch, nextTick } from 'vue'
import axios from 'axios'
const openApiSpec = ref(null)
const endpoints = ref([])
const selectedIndex = ref(null)
const selectedMethod = ref('')
const params = ref({})
const responseData = ref(null)
const errorMessage = ref('')
const jsonRequestBody = ref('')

const selectedEndpoint = computed(() => {
  if (selectedIndex.value !== null) {
    return endpoints.value[selectedIndex.value]
  }
  return null
})

const deficitResults = ref([])

const showTree = ref(false)
const svgWidth = '100%';
const svgHeight = '100%';
const svg = ref(null)

watch(showTree, async (visible) => {
  if (visible && responseData.value) {
    await nextTick()
    drawTree(responseData.value, svg.value)
  }
})

watch([selectedMethod, selectedEndpoint], () => {
  resetParams()
})

// Рекурсивная отрисовка дерева с кругами и стрелками
function drawTree(root, svgElement) {
  if (!svgElement) return;
  while (svgElement.firstChild) {
    svgElement.removeChild(svgElement.firstChild);
  }

  const nodeRadius = 50;
  const levelHeight = 100;
  const horizontalSpacing = 150;

  const nodesPositions = [];

  // Расчёт позиций узлов с layout, как у тебя было
  function layout(node, depth, offsetX) {
    let width = 0;
    if (!node.children || node.children.length === 0) {
      nodesPositions.push({ node, x: offsetX, y: depth * levelHeight });
      return nodeRadius * 2;
    }

    let childX = offsetX;
    for (const child of node.children) {
      const w = layout(child, depth + 1, childX);
      childX += w + horizontalSpacing;
      width += w + horizontalSpacing;
    }
    width -= horizontalSpacing; // убрать последний пробел

    // Позиция родителя — середина между детьми (сдвинута на -nodeRadius)
    const parentX = offsetX + width / 2 - nodeRadius;
    nodesPositions.push({ node, x: parentX, y: depth * levelHeight });

    return width;
  }

  layout(root, 0, nodeRadius);

  // Отрисовка линий (стрелок) между узлами
  for (const pos of nodesPositions) {
    if (!pos.node.children) continue;
    const posById = new Map(nodesPositions.map(p => [p.node.id, p]))

    for (const child of pos.node.children) {
      const childPos = posById.get(child.id)

      if (!childPos) continue;

      const x1 = pos.x + nodeRadius;
      const y1 = pos.y + nodeRadius;
      const x2 = childPos.x + nodeRadius;
      const y2 = childPos.y + nodeRadius;

      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", x1);
      line.setAttribute("y1", y1);
      line.setAttribute("x2", x2);
      line.setAttribute("y2", y2);
      line.setAttribute("stroke", "#666");
      line.setAttribute("stroke-width", 2);
      svgElement.appendChild(line);

      const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
      const midX = (x1 + x2) / 2;
      const midY = (y1 + y2) / 2;

      text.setAttribute("x", midX);
      text.setAttribute("y", midY - 5);
      text.setAttribute("text-anchor", "middle");
      text.setAttribute("fill", "#333");
      text.style.fontSize = "15px";
      text.style.userSelect = "none";

      text.textContent = child.quantity ?? "";

      svgElement.appendChild(text);
    }
  }

  // Отрисовка узлов (кругов) и подписей
  for (const pos of nodesPositions) {
    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");

    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", pos.x + nodeRadius);
    circle.setAttribute("cy", pos.y + nodeRadius);
    circle.setAttribute("r", nodeRadius);
    circle.setAttribute("fill", "#007acc");
    circle.setAttribute("stroke", "#004a99");
    circle.setAttribute("stroke-width", 2);
    g.appendChild(circle);

    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", pos.x + nodeRadius);
    text.setAttribute("y", pos.y + nodeRadius - 10);
    text.setAttribute("text-anchor", "middle");
    text.setAttribute("fill", "#fff");
    text.style.fontSize = "12px";
    text.style.userSelect = "none";

    const words = pos.node.name.split(" ");
    const lineHeight = 14;

    words.forEach((word, i) => {
      const tspan = document.createElementNS("http://www.w3.org/2000/svg", "tspan");
      tspan.setAttribute("x", pos.x + nodeRadius);
      tspan.setAttribute("dy", i === 0 ? "0" : lineHeight);
      tspan.textContent = word;
      text.appendChild(tspan);
    });

    g.appendChild(text);

    svgElement.appendChild(g);
  }

  // Автоматическое масштабирование SVG под содержимое
  const bbox = svgElement.getBBox();
  svgElement.setAttribute('viewBox', `${bbox.x} ${bbox.y} ${bbox.width} ${bbox.height}`);
  svgElement.setAttribute('width', bbox.width);
  svgElement.setAttribute('height', bbox.height);
}

function analyzeDeficit() {
  try {

    const data = jsonRequestBody.value ? JSON.parse(jsonRequestBody.value) : undefined
    const results = []

    const analyzeArray = (arr) => {
      for (const item of arr) {
        if (item.deficit > 0) {
          results.push({ name: item.name || 'Без имени', deficit: item.deficit })
        }
      }
    }

    if (Array.isArray(data)) {
      analyzeArray(data)
    } else if (Array.isArray(data.items)) {
      analyzeArray(data.items)
    } else {
      console.warn('Формат данных не распознан')
    }

    deficitResults.value = results
  } catch (e) {
    deficitResults.value = [{ name: 'Ошибка парсинга JSON', deficit: '' }]
  }
}


function selectEndpoint(index) {
  selectedIndex.value = index
  const ep = endpoints.value[index]
  if (ep && ep.methods.length > 0) {
    selectedMethod.value = ep.methods[0]
    resetParams()
  }
  clearResponse()
}
function resolveRef(ref) {
  if (!ref.startsWith('#/components/schemas/')) return null;
  const schemaName = ref.replace('#/components/schemas/', '');
  return openApiSpec.value.components?.schemas?.[schemaName] || null;
}

function buildExampleFromSchema(schema) {
  if (!schema) return null;

  // Если есть $ref — раскрываем её рекурсивно
  if (schema.$ref) {
    const resolved = resolveRef(schema.$ref);
    return buildExampleFromSchema(resolved);
  }

  // Возвращаем явный example или default, если есть
  if (schema.example !== undefined) return schema.example;
  if (schema.default !== undefined) return schema.default;

  // Для объектов — строим объект из свойств рекурсивно
  if (schema.type === 'object' && schema.properties) {
    const obj = {};
    for (const [key, propSchema] of Object.entries(schema.properties)) {
      obj[key] = buildExampleFromSchema(propSchema);
    }
    return obj;
  }

  // Для массивов — строим пример из items
  if (schema.type === 'array' && schema.items) {
    return [buildExampleFromSchema(schema.items)];
  }

  // Примитивы по типу
  switch (schema.type) {
    case 'string': return "string";
    case 'integer':
    case 'number': return 0;
    case 'boolean': return false;
    default: return null;
  }
}


function resetParams() {
  if (!selectedEndpoint.value || !selectedMethod.value) {
    params.value = {}
    jsonRequestBody.value = ''
    return
  }

  const path = selectedEndpoint.value.path
  const method = selectedMethod.value.toLowerCase()
  const op = openApiSpec.value.paths?.[path]?.[method]

  //console.log('resetParams called for:', path, method)
  //console.log('op:', op)

  const paramDefs = getParameters(openApiSpec.value, path, selectedMethod.value)
  const newParams = {}
  for (const p of paramDefs) {
    newParams[p.name] = ''
  }
  params.value = newParams

  if (method === 'post' || method === 'put') {
    const content = op?.requestBody?.content?.['application/json'];

    let example = content?.schema?.example || content?.example || Object.values(content?.examples || {})[0]?.value;

    if (example) {
      jsonRequestBody.value = JSON.stringify(example, null, 2)
    } else if (content?.schema) {
      const builtExample = buildExampleFromSchema(content.schema);
      jsonRequestBody.value = JSON.stringify(builtExample, null, 2)
    } else {
      jsonRequestBody.value = ''
    }
  }

}

// Парсим OpenAPI спецификацию и строим список эндпоинтов с методами
function buildEndpointsList(spec) {
  const eps = []
  for (const path in spec.paths) {
    const methods = Object.keys(spec.paths[path]).map(m => m.toUpperCase())
    eps.push({ path, methods })
  }
  return eps
}

// Возвращает параметры из OpenAPI для выбранного эндпоинта и метода
function getParameters(spec, path, method) {
  if (!spec.paths[path] || !spec.paths[path][method.toLowerCase()]) return []
  const op = spec.paths[path][method.toLowerCase()]
  let params = op.parameters || []

  // если есть requestBody — попытаемся добавить его как параметр
  if (op.requestBody && op.requestBody.content) {
    const content = op.requestBody.content['application/json']
    if (content && content.schema && content.schema.properties) {
      const bodyParams = Object.entries(content.schema.properties).map(([name, schema]) => ({
        name,
        in: 'body',
        description: schema.description || '',
        required: content.schema.required?.includes(name) || false,
        schema
      }))
      params = params.concat(bodyParams)
    }
  }

  return params
}

const parameterList = computed(() => {
  if (!selectedEndpoint.value || !selectedMethod.value || !openApiSpec.value) return []
  return getParameters(openApiSpec.value, selectedEndpoint.value.path, selectedMethod.value)
})

const formattedResponse = computed(() => {
  if (!responseData.value) return ''
  try {
    return JSON.stringify(responseData.value, null, 2)
  } catch {
    return String(responseData.value)
  }
})

function clearResponse() {
  responseData.value = null
  errorMessage.value = ''
}

async function sendRequest() {
  errorMessage.value = ''
  responseData.value = null
  deficitResults.value = []

  let dataToSend
  try {
    dataToSend = jsonRequestBody.value ? JSON.parse(jsonRequestBody.value) : undefined
  } catch (e) {
    errorMessage.value = 'Ошибка парсинга JSON тела запроса'
    return
  }

  if (!selectedEndpoint.value) {
    errorMessage.value = 'Метод не выбран'
    return
  }
  if (!selectedMethod.value) {
    errorMessage.value = 'HTTP метод не выбран'
    return
  }

  const baseURL = 'http://localhost:8000'
  let url = baseURL + selectedEndpoint.value.path

  const paramDefs = getParameters(openApiSpec.value, selectedEndpoint.value.path, selectedMethod.value)

  const queryParams = {}
  const pathParams = {}
  const bodyData = {}

  for (const p of paramDefs) {
    const val = params.value[p.name]
    if (val === '' || val === undefined) continue
    if (p.in === 'query') queryParams[p.name] = val
    else if (p.in === 'path') pathParams[p.name] = val
    else if (p.in === 'body') bodyData[p.name] = val
  }

  for (const key in pathParams) {
    url = url.replace(`{${key}}`, encodeURIComponent(pathParams[key]))
  }

  try {
    let res
    if (selectedMethod.value === 'GET' || selectedMethod.value === 'DELETE') {
      res = await axios({
        method: selectedMethod.value.toLowerCase(),
        url,
        params: queryParams
      })
    } else {
      res = await axios({
        method: selectedMethod.value.toLowerCase(),
        url,
        params: queryParams,
        data: dataToSend
      })
    }

    responseData.value = res.data

    // Автоматический анализ дефицита из ответа
    jsonRequestBody.value = JSON.stringify(res.data, null, 2)
    if (selectedEndpoint.value.path === "/analyze_deficit") {
      analyzeDeficit()
    }
  } catch (e) {
    errorMessage.value = e.response?.data || e.message
  }
}


onMounted(async () => {
  try {
    const spec = await axios.get('http://localhost:8000/openapi.json')
    openApiSpec.value = spec.data
    endpoints.value = buildEndpointsList(spec.data)

    if (endpoints.value.length > 0) {
      selectEndpoint(0)

        resetParams()
    }
  } catch (error) {
    console.error('Ошибка при загрузке OpenAPI:', error)
  }
})
</script>


<style scoped>
#json-body {
  height: 50%;
}
h4 {
  margin: 0;
}
.container {
  max-width: 900px;
  margin: 0 auto;
}
.flex {
  width:100%;
  display: flex;
  gap: 20px;
  margin-top: 20px;
}
.endpoint-list {
  list-style: none;
  padding: 0;
  width: 35%;
  border: 1px solid #ccc;
  border-radius: 5px;
  max-height: 500px;
  overflow-y: auto;
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
.endpoint-details {
  width: 65%;
  border: 1px solid #ccc;
  border-radius: 5px;
  padding: 16px;
  min-height: 350px;
  background-color: #fafafa;
  white-space: pre-wrap;
}

.right-side-blocks {
  display: flex;
  flex-direction: column;
  gap: 20px;
  margin-top: 15px;
  height: 500px;
}

.manual-analysis {
  width: 45%;
}
.data-block, .response-block {
  flex: 1 1 0;
  border: 1px solid #ccc;
  border-radius: 5px;
  padding: 12px;
  background-color: #fff;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  height: 100%;
}

.param-item {
  margin-bottom: 8px;
}

.response-content {
  flex: 1 1 0;
  overflow-y: auto;
  background-color: #f5f5f5;
  padding: 10px;
  border-radius: 4px;
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 100%;
}
.response-content pre {
  text-align: left;
}

.error-message {
  margin-top: 10px;
  color: red;
  max-height: 150px;
  overflow-y: auto;
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
