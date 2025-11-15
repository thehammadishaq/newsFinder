import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:9001'

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 10000, // 10 second timeout
})

// Add request interceptor for debugging
api.interceptors.request.use(
  (config) => {
    console.log(`[API] ${config.method?.toUpperCase()} ${config.url}`)
    return config
  },
  (error) => {
    console.error('[API] Request error:', error)
    return Promise.reject(error)
  }
)

// Add response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.code === 'ECONNABORTED') {
      console.error('[API] Request timeout')
    } else if (error.code === 'ERR_NETWORK') {
      console.error('[API] Network error - backend may not be running')
    } else {
      console.error('[API] Response error:', error.response?.status, error.response?.data)
    }
    return Promise.reject(error)
  }
)

// API Service Functions
export const apiService = {
  // Health check
  healthCheck: async () => {
    const response = await api.get('/health')
    return response.data
  },

  // Discover selectors
  discoverSelectors: async (data) => {
    const response = await api.post('/api/v1/discover', data)
    return response.data
  },

  discoverSelectorsSync: async (data) => {
    const response = await api.post('/api/v1/discover/sync', data)
    return response.data
  },

  // Scrape articles
  scrapeArticles: async (data) => {
    const response = await api.post('/api/v1/scrape', data)
    return response.data
  },

  scrapeArticlesSync: async (data) => {
    const response = await api.post('/api/v1/scrape/sync', data)
    return response.data
  },

  // Clean articles
  cleanArticles: async (data) => {
    const response = await api.post('/api/v1/clean', data)
    return response.data
  },

  cleanArticlesSync: async (data) => {
    const response = await api.post('/api/v1/clean/sync', data)
    return response.data
  },

  // Status
  getJobStatus: async (jobId) => {
    const response = await api.get(`/api/v1/status/${jobId}`)
    return response.data
  },

  getOverallStatus: async (params = {}) => {
    const response = await api.get('/api/v1/status', { params })
    return response.data
  },

  getSitesStatus: async (params = {}) => {
    const response = await api.get('/api/v1/sites', { params })
    return response.data
  },

  // Jobs
  listJobs: async (params = {}) => {
    const response = await api.get('/api/v1/jobs', { params })
    return response.data
  },

  deleteJob: async (jobId) => {
    const response = await api.delete(`/api/v1/jobs/${jobId}`)
    return response.data
  },

  // File operations
  uploadUrls: async (file) => {
    const formData = new FormData()
    formData.append('file', file)
    const response = await api.post('/api/v1/upload/urls', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    })
    return response.data
  },

  downloadFile: async (fileType) => {
    const response = await api.get(`/api/v1/download/${fileType}`, {
      responseType: 'blob',
    })
    return response.data
  },
}

export default apiService

