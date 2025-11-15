import { useState, useEffect } from 'react'
import { FileText, Sparkles, Activity, TrendingUp, CheckCircle, XCircle, Clock, ExternalLink } from 'lucide-react'
import apiService from '../services/api'

function DashboardPage() {
  const [status, setStatus] = useState(null)
  const [articles, setArticles] = useState([])
  const [articlesCount, setArticlesCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [articlesLoading, setArticlesLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    loadStatus()
    loadArticles()
    const interval = setInterval(() => {
      loadStatus()
      loadArticles()
    }, 10000) // Refresh every 10 seconds
    return () => clearInterval(interval)
  }, [])

  const loadArticles = async () => {
    try {
      setArticlesLoading(true)
      const [articlesData, countData] = await Promise.all([
        apiService.getArticles({ limit: 10 }),
        apiService.getArticlesCount(),
      ])
      setArticles(articlesData.articles || [])
      setArticlesCount(countData.count || 0)
    } catch (err) {
      // Don't show error for articles, just log
      console.error('Failed to load articles:', err)
    } finally {
      setArticlesLoading(false)
    }
  }

  const loadStatus = async () => {
    try {
      // First check if backend is reachable
      try {
        await apiService.healthCheck()
      } catch (healthErr) {
        setError(`Backend not reachable. Make sure the API is running on port 9001. Error: ${healthErr.message}`)
        setLoading(false)
        return
      }
      
      const data = await apiService.getOverallStatus()
      setStatus(data)
      setError(null)
    } catch (err) {
      const errorMsg = err.response?.data?.detail || err.message || 'Network Error'
      setError(`Failed to load status: ${errorMsg}. Make sure the backend is running on http://localhost:9001`)
    } finally {
      setLoading(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-black"></div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="space-y-4">
        <div className="bg-white border-2 border-black text-black px-4 py-3 rounded-lg">
          <div className="flex items-start">
            <XCircle className="h-5 w-5 mr-2 mt-0.5 flex-shrink-0" />
            <div>
              <p className="font-semibold">Error loading status</p>
              <p className="text-sm mt-1">{error}</p>
              <p className="text-sm mt-2">
                <strong>Backend URL:</strong> {import.meta.env.VITE_API_URL || 'http://localhost:9001'}
              </p>
              <p className="text-sm mt-1">
                Make sure the backend is running: <code className="bg-gray-100 px-2 py-1 rounded border border-gray-300">python run_api.py</code>
              </p>
            </div>
          </div>
        </div>
        <button
          onClick={loadStatus}
          className="px-4 py-2 bg-black text-white rounded-lg hover:bg-gray-800 border border-black"
        >
          Retry
        </button>
      </div>
    )
  }

  const stats = [
    {
      title: 'Articles in DB',
      value: articlesCount,
      icon: FileText,
      color: 'bg-gray-800',
    },
  ]

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
        <p className="mt-2 text-gray-600">Overview of your news scraping pipeline</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {stats.map((stat, index) => {
          const Icon = stat.icon
          return (
            <div
              key={index}
              className="bg-white rounded-lg shadow p-4 hover:shadow-lg transition-shadow"
            >
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs font-medium text-gray-600">{stat.title}</p>
                  <p className="text-2xl font-bold text-gray-900 mt-1">{stat.value}</p>
                </div>
                <div className={`${stat.color} p-2 rounded-full`}>
                  <Icon className="h-5 w-5 text-white" />
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* Recent Articles */}
      <div>
        <h2 className="text-xl font-bold text-gray-900 mb-3">Recent Articles</h2>
        {articlesLoading ? (
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-black mx-auto"></div>
            <p className="mt-2 text-sm text-gray-600">Loading articles...</p>
          </div>
        ) : articles.length > 0 ? (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="divide-y divide-gray-200">
              {articles.map((article, index) => (
                <div key={index} className="p-4 hover:bg-gray-50 transition-colors">
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <h3 className="text-base font-semibold text-gray-900 mb-1">
                        {article.title || 'Untitled'}
                      </h3>
                      {article.summary && (
                        <p className="text-xs text-gray-600 mb-2 line-clamp-2">
                          {article.summary}
                        </p>
                      )}
                      <div className="flex items-center gap-3 text-xs text-gray-500">
                        {article.source && (
                          <span className="font-medium text-gray-700">{article.source}</span>
                        )}
                        {article.date && (
                          <span className="flex items-center">
                            <Clock className="h-3 w-3 mr-1" />
                            {new Date(article.date).toLocaleDateString('en-US', {
                              year: 'numeric',
                              month: 'short',
                              day: 'numeric',
                              hour: '2-digit',
                              minute: '2-digit'
                            })}
                          </span>
                        )}
                      </div>
                    </div>
                    {article.url && (
                      <a
                        href={article.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="ml-3 p-1.5 text-gray-600 hover:text-black transition-colors"
                        title="Open article"
                      >
                        <ExternalLink className="h-4 w-4" />
                      </a>
                    )}
                  </div>
                </div>
              ))}
            </div>
            {articlesCount > 10 && (
              <div className="p-3 bg-gray-50 text-center border-t border-gray-200">
                <p className="text-xs text-gray-600">
                  Showing 10 of {articlesCount} articles
                </p>
              </div>
            )}
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <FileText className="h-10 w-10 text-gray-400 mx-auto mb-3" />
            <p className="text-sm text-gray-600">No articles found</p>
            <p className="text-xs text-gray-500 mt-1">
              Run the scraping and cleaning pipeline to see articles here
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

export default DashboardPage

