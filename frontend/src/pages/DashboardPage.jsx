import { useState, useEffect } from 'react'
import { Search, FileText, Sparkles, Activity, TrendingUp, CheckCircle, XCircle, Clock } from 'lucide-react'
import { Link } from 'react-router-dom'
import apiService from '../services/api'

function DashboardPage() {
  const [status, setStatus] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    loadStatus()
    const interval = setInterval(loadStatus, 5000) // Refresh every 5 seconds
    return () => clearInterval(interval)
  }, [])

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
      title: 'Total Sites',
      value: status?.total_sites || 0,
      icon: FileText,
      color: 'bg-black',
      link: '/status',
    },
    {
      title: 'Sites with Sitemap',
      value: status?.sites_with_sitemap || 0,
      icon: CheckCircle,
      color: 'bg-gray-800',
    },
    {
      title: 'Sites with CSS Only',
      value: status?.sites_with_css_only || 0,
      icon: Activity,
      color: 'bg-gray-700',
    },
    {
      title: 'Failed Sites',
      value: status?.sites_failed || 0,
      icon: XCircle,
      color: 'bg-gray-900',
    },
    {
      title: 'Raw Articles',
      value: status?.total_raw_articles || 0,
      icon: TrendingUp,
      color: 'bg-gray-600',
    },
    {
      title: 'Cleaned Articles',
      value: status?.total_cleaned_articles || 0,
      icon: Sparkles,
      color: 'bg-black',
    },
  ]

  const quickActions = [
    {
      title: 'Discover Selectors',
      description: 'Find sitemap and CSS selectors for news sites',
      icon: Search,
      link: '/discover',
      color: 'bg-black hover:bg-gray-800',
    },
    {
      title: 'Scrape Articles',
      description: 'Extract articles using discovered selectors',
      icon: FileText,
      link: '/scrape',
      color: 'bg-black hover:bg-gray-800',
    },
    {
      title: 'Clean Articles',
      description: 'Filter and deduplicate scraped articles',
      icon: Sparkles,
      link: '/clean',
      color: 'bg-black hover:bg-gray-800',
    },
    {
      title: 'View Status',
      description: 'Monitor pipeline status and jobs',
      icon: Activity,
      link: '/status',
      color: 'bg-black hover:bg-gray-800',
    },
  ]

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-black">Dashboard</h1>
        <p className="mt-2 text-gray-700">Overview of your news scraping pipeline</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {stats.map((stat, index) => {
          const Icon = stat.icon
          return (
            <div
              key={index}
              className="bg-white rounded-lg shadow p-6 hover:shadow-lg transition-shadow"
            >
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">{stat.title}</p>
                  <p className="text-3xl font-bold text-gray-900 mt-2">{stat.value}</p>
                </div>
                <div className={`${stat.color} p-3 rounded-full`}>
                  <Icon className="h-6 w-6 text-white" />
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* Quick Actions */}
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-4">Quick Actions</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {quickActions.map((action, index) => {
            const Icon = action.icon
            return (
              <Link
                key={index}
                to={action.link}
                className={`${action.color} text-white rounded-lg p-6 hover:shadow-lg transition-all transform hover:scale-105`}
              >
                <Icon className="h-8 w-8 mb-3" />
                <h3 className="text-lg font-semibold mb-2">{action.title}</h3>
                <p className="text-sm opacity-90">{action.description}</p>
              </Link>
            )
          })}
        </div>
      </div>

      {/* Recent Sites */}
      {status?.sites && status.sites.length > 0 && (
        <div>
          <h2 className="text-2xl font-bold text-gray-900 mb-4">Recent Sites</h2>
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Domain
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Raw Articles
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Cleaned Articles
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {status.sites.slice(0, 10).map((site, index) => (
                  <tr key={index} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      {site.domain || 'N/A'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                    <span
                      className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full border ${
                        site.overall_status === 'Success'
                          ? 'bg-white text-black border-black'
                          : site.overall_status === 'Error'
                          ? 'bg-black text-white border-black'
                          : 'bg-gray-100 text-black border-gray-300'
                      }`}
                    >
                      {site.overall_status || 'Pending'}
                    </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {site.raw_articles_count || 0}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {site.cleaned_articles_count || 0}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

export default DashboardPage

