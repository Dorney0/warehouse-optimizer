import axios from 'axios'

const API_URL = 'http://127.0.0.1:8000'

export interface Endpoint {
    path: string
    method: string
}

export async function getEndpoints(): Promise<Endpoint[]> {
    const response = await axios.get<Endpoint[]>(`${API_URL}/api/endpoints`)
    return response.data
}
