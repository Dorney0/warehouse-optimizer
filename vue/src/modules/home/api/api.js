import axios from 'axios'
import {API_URL} from '@/api/url'
export async function fetch_entities_with_children() {
    try {
        const response = await axios.get(`${API_URL}/entities_with_children/`)
        return response.data
    } catch (error) {
        console.error('Ошибка при получении entities:', error)
        return []
    }
}