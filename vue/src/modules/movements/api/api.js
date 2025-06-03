import axios from 'axios'
import {API_URL} from '@/api/url'
export async function fetchMovements() {
    try {
        const response = await axios.get(`${API_URL}/stock_movements/`)
        return response.data
    } catch (error) {
        console.error('Ошибка при получении entities:', error)
        return []
    }
}
