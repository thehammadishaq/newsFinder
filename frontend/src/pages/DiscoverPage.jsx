import { useState, useEffect } from 'react'
import { Search, Upload, Loader2, CheckCircle, XCircle, AlertCircle, FileText, Activity } from 'lucide-react'
import apiService from '../services/api'

function DiscoverPage() {
  const [urls, setUrls] = useState([''])
  const [recentHours, setRecentHours] = useState(24)
  const [siteConcurrency, setSiteConcurrency] = useState(1)
  const [llmConcurrency, setLlmConcurrency] = useState(3)
  const [timeout, setTimeout] = useState(15.0)
  const [maxDepth, setMaxDepth] = useState(2)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [useSync, setUseSync] = useState(false)
  const [status, setStatus] = useState(null)
  const [statusLoading, setStatusLoading] = useState(true)

  const addUrlField = () => {
    setUrls([...urls, ''])
  }

  const removeUrlField = (index) => {
    setUrls(urls.filter((_, i) => i !== index))
  }

  const updateUrl = (index, value) => {
    const newUrls = [...urls]
    newUrls[index] = value
    setUrls(newUrls)
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError(null)
    setResult(null)

    const validUrls = urls.filter(url => url.trim() !== '')
    if (validUrls.length === 0) {
      setError('Please enter at least one URL')
      setLoading(false)
      return
    }

    try {
      const data = {
        urls: validUrls,
        recent_hours: recentHours,
        site_concurrency: siteConcurrency,
        llm_concurrency: llmConcurrency,
        timeout: timeout,
        max_depth: maxDepth,
      }

      const response = useSync
        ? await apiService.discoverSelectorsSync(data)
        : await apiService.discoverSelectors(data)

      setResult(response)
      
      if (!useSync && response.job_id) {
        // Poll for job status
        pollJobStatus(response.job_id)
      } else if (useSync && response.status === 'completed') {
        // If sync mode and completed, refresh status immediately
        loadStatus()
      }
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'An error occurred')
    } finally {
      if (useSync) {
        setLoading(false)
      }
    }
  }

  const pollJobStatus = async (jobId) => {
    const interval = setInterval(async () => {
      try {
        const status = await apiService.getJobStatus(jobId)
        if (status.status === 'completed' || status.status === 'failed') {
          clearInterval(interval)
          setResult(status)
          setLoading(false)
          // Refresh status after discovery completes
          if (status.status === 'completed') {
            loadStatus()
          }
        }
      } catch (err) {
        clearInterval(interval)
        setError(err.message)
        setLoading(false)
      }
    }, 2000)
  }

  const handleFileUpload = async (e) => {
    const file = e.target.files[0]
    if (!file) return

    try {
      const response = await apiService.uploadUrls(file)
      setResult({ message: 'File uploaded successfully', file_path: response.file_path })
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Upload failed')
    }
  }

  useEffect(() => {
    loadStatus()
    const interval = setInterval(() => {
      loadStatus()
    }, 10000) // Refresh every 10 seconds
    return () => clearInterval(interval)
  }, [])

  const loadStatus = async () => {
    try {
      const data = await apiService.getOverallStatus()
      setStatus(data)
    } catch (err) {
      // Don't show error, just log
      console.error('Failed to load status:', err)
    } finally {
      setStatusLoading(false)
    }
  }

  const stats = [
    {
      title: 'Total Sites',
      value: status?.total_sites || 0,
      icon: FileText,
      color: 'bg-black',
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
  ]

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Discover Selectors</h1>
        <p className="mt-2 text-gray-600">Find sitemap and CSS selectors for news websites</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
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

      <div className="bg-white rounded-lg shadow p-6">
        <form onSubmit={handleSubmit} className="space-y-6">
          {/* URLs */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              URLs <span className="text-black font-bold">*</span>
            </label>
            <div className="space-y-2">
              {urls.map((url, index) => (
                <div key={index} className="flex gap-2">
                  <input
                    type="url"
                    value={url}
                    onChange={(e) => updateUrl(index, e.target.value)}
                    placeholder="https://www.example.com"
                    className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
                    required={index === 0}
                  />
                  {urls.length > 1 && (
                    <button
                      type="button"
                      onClick={() => removeUrlField(index)}
                      className="px-4 py-2 bg-black text-white rounded-lg hover:bg-gray-800 border border-black"
                    >
                      Remove
                    </button>
                  )}
                </div>
              ))}
              <button
                type="button"
                onClick={addUrlField}
                className="text-black hover:text-gray-700 text-sm font-medium underline"
              >
                + Add another URL
              </button>
            </div>
          </div>

          {/* Settings */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Recent Hours
              </label>
              <input
                type="number"
                value={recentHours}
                onChange={(e) => setRecentHours(parseInt(e.target.value))}
                min="1"
                max="168"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Site Concurrency
              </label>
              <input
                type="number"
                value={siteConcurrency}
                onChange={(e) => setSiteConcurrency(parseInt(e.target.value))}
                min="1"
                max="10"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                LLM Concurrency
              </label>
              <input
                type="number"
                value={llmConcurrency}
                onChange={(e) => setLlmConcurrency(parseInt(e.target.value))}
                min="1"
                max="10"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Timeout (seconds)
              </label>
              <input
                type="number"
                value={timeout}
                onChange={(e) => setTimeout(parseFloat(e.target.value))}
                min="1"
                max="120"
                step="0.1"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Max Depth
              </label>
              <input
                type="number"
                value={maxDepth}
                onChange={(e) => setMaxDepth(parseInt(e.target.value))}
                min="1"
                max="5"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>
          </div>

          {/* Options */}
          <div className="flex items-center space-x-4">
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={useSync}
                onChange={(e) => setUseSync(e.target.checked)}
                className="mr-2"
              />
              <span className="text-sm text-gray-700">Use synchronous mode (for testing)</span>
            </label>
          </div>

          {/* File Upload */}
          <div className="border-t pt-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Or upload Excel file with URLs
            </label>
            <div className="flex items-center space-x-4">
              <label className="flex items-center px-4 py-2 bg-gray-100 rounded-lg cursor-pointer hover:bg-gray-200">
                <Upload className="h-5 w-5 mr-2" />
                <span>Choose File</span>
                <input
                  type="file"
                  accept=".xlsx,.xls"
                  onChange={handleFileUpload}
                  className="hidden"
                />
              </label>
            </div>
          </div>

          {/* Submit */}
          <button
            type="submit"
            disabled={loading}
            className="w-full bg-black text-white py-3 px-6 rounded-lg font-medium hover:bg-gray-800 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center border border-black"
          >
            {loading ? (
              <>
                <Loader2 className="animate-spin h-5 w-5 mr-2" />
                Discovering...
              </>
            ) : (
              <>
                <Search className="h-5 w-5 mr-2" />
                Discover Selectors
              </>
            )}
          </button>
        </form>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-white border-2 border-black text-black px-4 py-3 rounded-lg flex items-center">
          <XCircle className="h-5 w-5 mr-2" />
          {error}
        </div>
      )}

      {/* Result */}
      {result && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Result</h2>
          {result.job_id && (
            <div className="mb-4 p-4 bg-gray-50 border border-gray-300 rounded-lg">
              <p className="text-sm text-black">
                <strong>Job ID:</strong> {result.job_id}
              </p>
              <p className="text-sm text-black">
                <strong>Status:</strong> {result.status}
              </p>
            </div>
          )}
          <pre className="bg-gray-50 p-4 rounded-lg overflow-auto text-sm">
            {JSON.stringify(result, null, 2)}
          </pre>
        </div>
      )}

      {/* Recent Sites */}
      {status?.sites && status.sites.length > 0 && (
        <div>
          <h2 className="text-xl font-bold text-gray-900 mb-3">Recent Sites</h2>
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Domain
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Raw Articles
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Cleaned Articles
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {status.sites.slice(0, 10).map((site, index) => (
                  <tr key={index} className="hover:bg-gray-50">
                    <td className="px-4 py-2 whitespace-nowrap text-sm font-medium text-gray-900">
                      {site.domain || 'N/A'}
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap">
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
                    <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                      {site.raw_articles_count || 0}
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
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

export default DiscoverPage

