import axios from 'axios'
const API_URL = 'http://127.0.0.1:8000'
export async function fetchOrders() {
    try {
        const response = await axios.get(`${API_URL}/orders`)
        return response.data
    } catch (error) {
        console.error('Ошибка при получении entities:', error)
        return []
    }
}
