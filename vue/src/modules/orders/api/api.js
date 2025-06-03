import axios from 'axios'
import {API_URL} from '@/api/url'
export async function fetchOrders() {
    try {
        const response = await axios.get(`${API_URL}/orders/`)
        return response.data
    } catch (error) {
        console.error('Ошибка при получении entities:', error)
        return []
    }
}
