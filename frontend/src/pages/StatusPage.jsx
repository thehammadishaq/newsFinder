import { useState, useEffect } from 'react'
import { Activity, RefreshCw, Trash2, CheckCircle, XCircle, Clock, Search } from 'lucide-react'
import apiService from '../services/api'

function StatusPage() {
  const [status, setStatus] = useState(null)
  const [jobs, setJobs] = useState([])
  const [sites, setSites] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [activeTab, setActiveTab] = useState('overview')
  const [domainFilter, setDomainFilter] = useState('')

  useEffect(() => {
    loadData()
    const interval = setInterval(loadData, 5000)
    return () => clearInterval(interval)
  }, [domainFilter])

  const loadData = async () => {
    try {
      const [statusData, jobsData, sitesData] = await Promise.all([
        apiService.getOverallStatus({ domain: domainFilter || undefined }),
        apiService.listJobs(),
        apiService.getSitesStatus({ domain: domainFilter || undefined }),
      ])
      setStatus(statusData)
      setJobs(jobsData)
      setSites(sitesData)
      setError(null)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleDeleteJob = async (jobId) => {
    if (!confirm('Are you sure you want to delete this job?')) return
    
    try {
      await apiService.deleteJob(jobId)
      loadData()
    } catch (err) {
      setError(err.message)
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'completed':
      case 'Success':
        return 'bg-white text-black border border-black'
      case 'failed':
      case 'Error':
        return 'bg-black text-white border border-black'
      case 'running':
        return 'bg-gray-200 text-black border border-gray-400'
      default:
        return 'bg-gray-100 text-black border border-gray-300'
    }
  }

  if (loading && !status) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-black"></div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Status & Monitoring</h1>
          <p className="mt-2 text-gray-600">Monitor pipeline status, jobs, and sites</p>
        </div>
        <button
          onClick={loadData}
          className="flex items-center px-4 py-2 bg-black text-white rounded-lg hover:bg-gray-800 border border-black"
        >
          <RefreshCw className="h-5 w-5 mr-2" />
          Refresh
        </button>
      </div>

      {/* Filter */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="flex items-center space-x-4">
          <Search className="h-5 w-5 text-gray-400" />
          <input
            type="text"
            value={domainFilter}
            onChange={(e) => setDomainFilter(e.target.value)}
            placeholder="Filter by domain..."
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500"
          />
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200">
        <nav className="flex space-x-8">
          {['overview', 'jobs', 'sites'].map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === tab
                  ? 'border-black text-black'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab.charAt(0).toUpperCase() + tab.slice(1)}
            </button>
          ))}
        </nav>
      </div>

      {/* Overview Tab */}
      {activeTab === 'overview' && status && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
              <p className="text-sm font-medium text-gray-600">Total Sites</p>
              <p className="text-3xl font-bold text-black mt-2">
                {status.total_sites || 0}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
              <p className="text-sm font-medium text-gray-600">With Sitemap</p>
              <p className="text-3xl font-bold text-black mt-2">
                {status.sites_with_sitemap || 0}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
              <p className="text-sm font-medium text-gray-600">CSS Only</p>
              <p className="text-3xl font-bold text-black mt-2">
                {status.sites_with_css_only || 0}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
              <p className="text-sm font-medium text-gray-600">Failed</p>
              <p className="text-3xl font-bold text-black mt-2">
                {status.sites_failed || 0}
              </p>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
              <p className="text-sm font-medium text-gray-600">Raw Articles</p>
              <p className="text-3xl font-bold text-black mt-2">
                {status.total_raw_articles || 0}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
              <p className="text-sm font-medium text-gray-600">Cleaned Articles</p>
              <p className="text-3xl font-bold text-black mt-2">
                {status.total_cleaned_articles || 0}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Jobs Tab */}
      {activeTab === 'jobs' && (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Job ID
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Type
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Progress
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Created
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {jobs.map((job) => (
                <tr key={job.job_id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-900">
                    {job.job_id.substring(0, 20)}...
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {job.type}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`px-2 py-1 text-xs font-semibold rounded-full ${getStatusColor(job.status)}`}>
                      {job.status}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
                        <div
                          className="bg-black h-2 rounded-full"
                          style={{ width: `${job.progress}%` }}
                        ></div>
                      </div>
                      <span className="text-sm text-gray-600">{job.progress}%</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {new Date(job.created_at).toLocaleString()}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">
                    <button
                      onClick={() => handleDeleteJob(job.job_id)}
                      className="text-black hover:text-gray-700"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {jobs.length === 0 && (
            <div className="text-center py-12 text-gray-500">No jobs found</div>
          )}
        </div>
      )}

      {/* Sites Tab */}
      {activeTab === 'sites' && (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Domain
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Sitemap Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  CSS Fallback
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Extraction Path
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Raw Articles
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Cleaned Articles
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Overall Status
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {sites.map((site, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {site.domain}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {site.sitemap_status}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {site.css_fallback_status}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {site.extraction_path}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {site.raw_articles_count}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {site.cleaned_articles_count}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`px-2 py-1 text-xs font-semibold rounded-full ${getStatusColor(site.overall_status)}`}>
                      {site.overall_status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {sites.length === 0 && (
            <div className="text-center py-12 text-gray-500">No sites found</div>
          )}
        </div>
      )}

      {error && (
        <div className="bg-white border-2 border-black text-black px-4 py-3 rounded-lg">
          {error}
        </div>
      )}
    </div>
  )
}

export default StatusPage

