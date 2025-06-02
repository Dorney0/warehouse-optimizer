import axios from 'axios'
const API_URL = 'http://127.0.0.1:8000'
export async function fetchEntities() {
    try {
        const response = await axios.get(`${API_URL}/entities`)
        return response.data
    } catch (error) {
        console.error('Ошибка при получении entities:', error)
        return []
    }
}
