import axios from 'axios'
import {API_URL} from '@/api/url'
export async function fetchEntities() {
    try {
        const response = await axios.get(`${API_URL}/entity-stocks`)
        return response.data
    } catch (error) {
        console.error('Ошибка при получении entities:', error)
        return []
    }
}
