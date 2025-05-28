import axios from 'axios'

const API_URL = 'http://127.0.0.1:8000'

export async function GetEndpoints() {
    const response = await axios.get(`${API_URL}/api/endpoints`)
    return response.data
}
