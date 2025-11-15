import { useState, useEffect } from 'react'
import { FileText, Loader2, CheckCircle, XCircle, Download, Upload, Sparkles, TrendingUp } from 'lucide-react'
import apiService from '../services/api'

function ScrapePage() {
  const [streamPath, setStreamPath] = useState('')
  const [targetsJson, setTargetsJson] = useState('')
  const [uploadedFile, setUploadedFile] = useState(null)
  const [uploading, setUploading] = useState(false)
  const [fetchingUrl, setFetchingUrl] = useState(false)
  const [useTargetsJson, setUseTargetsJson] = useState(false)
  const [mode, setMode] = useState('auto')
  const [siteConcurrency, setSiteConcurrency] = useState(1)
  const [targetConcurrency, setTargetConcurrency] = useState(6)
  const [timeout, setTimeout] = useState(15.0)
  const [maxItems, setMaxItems] = useState(500)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [useSync, setUseSync] = useState(false)
  
  // Cleaning state
  const [cleanInputPath, setCleanInputPath] = useState('')
  const [cleaning, setCleaning] = useState(false)
  const [cleanResult, setCleanResult] = useState(null)
  const [cleanError, setCleanError] = useState(null)
  const [useCleanSync, setUseCleanSync] = useState(false)
  
  // Status state for stats
  const [status, setStatus] = useState(null)

  const handleFileUpload = async (e) => {
    const file = e.target.files[0]
    if (!file) return

    // Validate file type
    if (!file.name.endsWith('.jsonl') && !file.name.endsWith('.json')) {
      setError('Please upload a JSONL or JSON file')
      return
    }

    setUploading(true)
    setError(null)

    try {
      const response = await apiService.uploadStreamFile(file)
      setUploadedFile(response.filename)
      
      // Determine if it's a targets JSON file:
      // 1. If currently in targets JSON mode, treat as targets JSON
      // 2. If filename contains "targets" or "target", treat as targets JSON
      // 3. Otherwise, treat as stream file
      const isTargetsJson = useTargetsJson || 
                           file.name.includes('targets') || 
                           file.name.includes('target')
      
      if (isTargetsJson) {
        setTargetsJson(response.file_path)
        setUseTargetsJson(true)
      } else {
        setStreamPath(response.file_path)
        setUseTargetsJson(false)
      }
      setError(null)
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'File upload failed')
    } finally {
      setUploading(false)
    }
  }

  const handleUrlFetch = async () => {
    const url = streamPath.trim()
    if (!url) return

    // Check if it's a URL
    if (!url.startsWith('http://') && !url.startsWith('https://')) {
      return // Not a URL, treat as local path
    }

    // Validate URL format
    try {
      new URL(url)
    } catch {
      setError('Invalid URL format')
      return
    }

    setFetchingUrl(true)
    setError(null)
    setUploadedFile(null)

    try {
      const response = await apiService.fetchStreamFromUrl(url)
      setUploadedFile(response.filename)
      setStreamPath(response.file_path)
      setError(null)
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Failed to fetch file from URL')
    } finally {
      setFetchingUrl(false)
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError(null)
    setResult(null)

    try {
      const data = {
        stream_path: useTargetsJson ? undefined : (streamPath || undefined),
        targets_json: useTargetsJson ? (targetsJson || undefined) : undefined,
        mode: mode,
        site_concurrency: siteConcurrency,
        target_concurrency: targetConcurrency,
        timeout: timeout,
        max_items: maxItems,
      }

      const response = useSync
        ? await apiService.scrapeArticlesSync(data)
        : await apiService.scrapeArticles(data)

      setResult(response)
      
      if (!useSync && response.job_id) {
        pollJobStatus(response.job_id)
      } else if (useSync && response.status === 'completed') {
        // If sync mode and completed, refresh status immediately
        loadStatus()
      }
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'An error occurred')
      setLoading(false)
    } finally {
      if (useSync) {
        setLoading(false)
      }
    }
  }

  const pollJobStatus = async (jobId) => {
    const interval = setInterval(async () => {
      try {
        const jobStatus = await apiService.getJobStatus(jobId)
        if (jobStatus.status === 'completed' || jobStatus.status === 'failed') {
          clearInterval(interval)
          setResult(jobStatus)
          setLoading(false)
          // Refresh status after scraping completes
          if (jobStatus.status === 'completed') {
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

  const handleDownload = async (fileType) => {
    try {
      const blob = await apiService.downloadFile(fileType)
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = fileType === 'articles' ? 'articles.jsonl' : 
                   fileType === 'cleaned' ? 'cleaned.jsonl' : 
                   'selectors.jsonl'
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Download failed')
    }
  }

  const handleClean = async (e) => {
    e.preventDefault()
    setCleaning(true)
    setCleanError(null)
    setCleanResult(null)

    try {
      const data = {
        input_path: cleanInputPath || undefined,
      }

      const response = useCleanSync
        ? await apiService.cleanArticlesSync(data)
        : await apiService.cleanArticles(data)

      setCleanResult(response)
      
      if (!useCleanSync && response.job_id) {
        pollCleanJobStatus(response.job_id)
      } else if (useCleanSync && response.status === 'completed') {
        // If sync mode and completed, refresh status immediately
        loadStatus()
      }
    } catch (err) {
      setCleanError(err.response?.data?.detail || err.message || 'An error occurred')
      setCleaning(false)
    } finally {
      if (useCleanSync) {
        setCleaning(false)
      }
    }
  }

  const pollCleanJobStatus = async (jobId) => {
    const interval = setInterval(async () => {
      try {
        const jobStatus = await apiService.getJobStatus(jobId)
        if (jobStatus.status === 'completed' || jobStatus.status === 'failed') {
          clearInterval(interval)
          setCleanResult(jobStatus)
          setCleaning(false)
          // Refresh status after cleaning completes
          if (jobStatus.status === 'completed') {
            loadStatus()
          }
        }
      } catch (err) {
        clearInterval(interval)
        setCleanError(err.message)
        setCleaning(false)
      }
    }, 2000)
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
    }
  }

  const stats = [
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

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Scrape Articles</h1>
        <p className="mt-2 text-gray-600">Extract articles using discovered selectors</p>
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
          <div className="flex items-center space-x-4 mb-4">
            <label className="flex items-center">
              <input
                type="radio"
                checked={!useTargetsJson}
                onChange={() => setUseTargetsJson(false)}
                className="mr-2"
              />
              <span className="text-sm text-gray-700">Use Stream File (JSONL)</span>
            </label>
            <label className="flex items-center">
              <input
                type="radio"
                checked={useTargetsJson}
                onChange={() => setUseTargetsJson(true)}
                className="mr-2"
              />
              <span className="text-sm text-gray-700">Use Targets JSON</span>
            </label>
          </div>

          {!useTargetsJson ? (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Stream Path (optional)
            </label>
            <div className="space-y-3">
              <div className="flex gap-2">
            <input
              type="text"
              value={streamPath}
              onChange={(e) => setStreamPath(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && streamPath.trim().startsWith('http')) {
                      e.preventDefault()
                      handleUrlFetch()
                    }
                  }}
                  placeholder="selection_extraction_report_stream.jsonl or https://example.com/file.jsonl"
                  className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
                />
                {streamPath.trim().startsWith('http://') || streamPath.trim().startsWith('https://') ? (
                  <button
                    type="button"
                    onClick={handleUrlFetch}
                    disabled={fetchingUrl}
                    className="px-4 py-2 bg-black text-white rounded-lg hover:bg-gray-800 disabled:bg-gray-400 disabled:cursor-not-allowed border border-black flex items-center"
                  >
                    {fetchingUrl ? (
                      <>
                        <Loader2 className="animate-spin h-4 w-4 mr-2" />
                        Fetching...
                      </>
                    ) : (
                      <>
                        <Download className="h-4 w-4 mr-2" />
                        Fetch
                      </>
                    )}
                  </button>
                ) : null}
              </div>
              <div className="flex items-center gap-4">
                <div className="flex-1 border-t border-gray-300"></div>
                <span className="text-sm text-gray-500">OR</span>
                <div className="flex-1 border-t border-gray-300"></div>
              </div>
              <div>
                <label className="flex flex-col items-center justify-center w-full h-32 border-2 border-gray-300 border-dashed rounded-lg cursor-pointer bg-gray-50 hover:bg-gray-100">
                  <div className="flex flex-col items-center justify-center pt-5 pb-6">
                    {uploading ? (
                      <>
                        <Loader2 className="w-8 h-8 mb-2 text-gray-500 animate-spin" />
                        <p className="mb-2 text-sm text-gray-500">Uploading...</p>
                      </>
                    ) : (
                      <>
                        <Upload className="w-8 h-8 mb-2 text-gray-500" />
                        <p className="mb-2 text-sm text-gray-500">
                          <span className="font-semibold">Click to upload</span> or drag and drop
                        </p>
                        <p className="text-xs text-gray-500">JSONL or JSON file</p>
                      </>
                    )}
                  </div>
                  <input
                    type="file"
                    className="hidden"
                    accept=".jsonl,.json"
                    onChange={handleFileUpload}
                    disabled={uploading}
                  />
                </label>
                {uploadedFile && (
                  <div className="mt-2 p-2 bg-green-50 border border-green-200 rounded-lg">
                    <p className="text-sm text-green-700">
                      <CheckCircle className="inline h-4 w-4 mr-1" />
                      Uploaded: {uploadedFile}
                    </p>
                  </div>
                )}
              </div>
            </div>
            <p className="mt-1 text-sm text-gray-500">
              Enter a local file path, paste a URL to fetch online JSON/JSONL file, or upload a file. Leave empty to use default stream file.
            </p>
          </div>
          ) : (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Targets JSON Path (optional)
            </label>
            <div className="space-y-3">
              <div className="flex gap-2">
                <input
                  type="text"
                  value={targetsJson}
                  onChange={(e) => setTargetsJson(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && targetsJson.trim().startsWith('http')) {
                      e.preventDefault()
                      const url = targetsJson.trim()
                      setFetchingUrl(true)
                      apiService.fetchStreamFromUrl(url)
                        .then(response => {
                          setTargetsJson(response.file_path)
                          setFetchingUrl(false)
                        })
                        .catch(err => {
                          setError(err.response?.data?.detail || err.message || 'Failed to fetch file from URL')
                          setFetchingUrl(false)
                        })
                    }
                  }}
                  placeholder="selection_extraction_targets.json or https://example.com/targets.json"
                  className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
                />
                {targetsJson.trim().startsWith('http://') || targetsJson.trim().startsWith('https://') ? (
                  <button
                    type="button"
                    onClick={() => {
                      const url = targetsJson.trim()
                      setFetchingUrl(true)
                      apiService.fetchStreamFromUrl(url)
                        .then(response => {
                          setTargetsJson(response.file_path)
                          setFetchingUrl(false)
                        })
                        .catch(err => {
                          setError(err.response?.data?.detail || err.message || 'Failed to fetch file from URL')
                          setFetchingUrl(false)
                        })
                    }}
                    disabled={fetchingUrl}
                    className="px-4 py-2 bg-black text-white rounded-lg hover:bg-gray-800 disabled:bg-gray-400 disabled:cursor-not-allowed border border-black flex items-center"
                  >
                    {fetchingUrl ? (
                      <>
                        <Loader2 className="animate-spin h-4 w-4 mr-2" />
                        Fetching...
                      </>
                    ) : (
                      <>
                        <Download className="h-4 w-4 mr-2" />
                        Fetch
                      </>
                    )}
                  </button>
                ) : null}
              </div>
              <div className="flex items-center gap-4">
                <div className="flex-1 border-t border-gray-300"></div>
                <span className="text-sm text-gray-500">OR</span>
                <div className="flex-1 border-t border-gray-300"></div>
              </div>
              <div>
                <label className="flex flex-col items-center justify-center w-full h-32 border-2 border-gray-300 border-dashed rounded-lg cursor-pointer bg-gray-50 hover:bg-gray-100">
                  <div className="flex flex-col items-center justify-center pt-5 pb-6">
                    {uploading ? (
                      <>
                        <Loader2 className="w-8 h-8 mb-2 text-gray-500 animate-spin" />
                        <p className="mb-2 text-sm text-gray-500">Uploading...</p>
                      </>
                    ) : (
                      <>
                        <Upload className="w-8 h-8 mb-2 text-gray-500" />
                        <p className="mb-2 text-sm text-gray-500">
                          <span className="font-semibold">Click to upload</span> or drag and drop
                        </p>
                        <p className="text-xs text-gray-500">JSON file (targets format)</p>
                      </>
                    )}
                  </div>
                  <input
                    type="file"
                    className="hidden"
                    accept=".json"
                    onChange={handleFileUpload}
                    disabled={uploading}
                  />
                </label>
                {uploadedFile && useTargetsJson && (
                  <div className="mt-2 p-2 bg-green-50 border border-green-200 rounded-lg">
                    <p className="text-sm text-green-700">
                      <CheckCircle className="inline h-4 w-4 mr-1" />
                      Uploaded: {uploadedFile}
                    </p>
                  </div>
                )}
              </div>
            </div>
            <p className="mt-1 text-sm text-gray-500">
              Enter a local file path, paste a URL to fetch online JSON file, or upload a file. Leave empty to use default.
            </p>
          </div>
          )}

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Mode
            </label>
            <select
              value={mode}
              onChange={(e) => setMode(e.target.value)}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500"
            >
              <option value="auto">Auto</option>
              <option value="sitemap">Sitemap Only</option>
              <option value="css">CSS Only</option>
              <option value="both">Both</option>
            </select>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Site Concurrency
              </label>
              <input
                type="number"
                value={siteConcurrency}
                onChange={(e) => {
                  const val = parseInt(e.target.value) || 1
                  setSiteConcurrency(val > 0 ? val : 1)
                }}
                min="1"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Target Concurrency
              </label>
              <input
                type="number"
                value={targetConcurrency}
                onChange={(e) => {
                  const val = parseInt(e.target.value) || 1
                  setTargetConcurrency(val > 0 ? val : 1)
                }}
                min="1"
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
                onChange={(e) => {
                  const val = parseFloat(e.target.value) || 15.0
                  setTimeout(val > 0 ? val : 15.0)
                }}
                min="0.1"
                step="0.1"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Max Items
              </label>
              <input
                type="number"
                value={maxItems}
                onChange={(e) => {
                  const val = parseInt(e.target.value) || 1
                  setMaxItems(val > 0 ? val : 1)
                }}
                min="1"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
            </div>
          </div>

          <div className="flex items-center space-x-4">
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={useSync}
                onChange={(e) => setUseSync(e.target.checked)}
                className="mr-2"
              />
              <span className="text-sm text-gray-700">Use synchronous mode</span>
            </label>
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-black text-white py-3 px-6 rounded-lg font-medium hover:bg-gray-800 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center border border-black"
          >
            {loading ? (
              <>
                <Loader2 className="animate-spin h-5 w-5 mr-2" />
                Scraping...
              </>
            ) : (
              <>
                <FileText className="h-5 w-5 mr-2" />
                Start Scraping
              </>
            )}
          </button>
        </form>
      </div>

      {error && (
        <div className="bg-white border-2 border-black text-black px-4 py-3 rounded-lg flex items-center">
          <XCircle className="h-5 w-5 mr-2" />
          {error}
        </div>
      )}

      {result && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Scraping Result</h2>
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

      {/* Clean Articles Section */}
      <div className="border-t pt-6 mt-6">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 mb-2">Clean Articles</h2>
          <p className="text-gray-600 mb-4">Filter, deduplicate, and clean scraped articles</p>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <form onSubmit={handleClean} className="space-y-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Input Path (optional)
              </label>
              <input
                type="text"
                value={cleanInputPath}
                onChange={(e) => setCleanInputPath(e.target.value)}
                placeholder="stream_scraped_articles.jsonl"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-black focus:border-black"
              />
              <p className="mt-1 text-sm text-gray-500">
                Leave empty to use default input file
              </p>
            </div>

            <div className="flex items-center space-x-4">
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={useCleanSync}
                  onChange={(e) => setUseCleanSync(e.target.checked)}
                  className="mr-2"
                />
                <span className="text-sm text-gray-700">Use synchronous mode</span>
              </label>
            </div>

            <button
              type="submit"
              disabled={cleaning}
              className="w-full bg-black text-white py-3 px-6 rounded-lg font-medium hover:bg-gray-800 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center border border-black"
            >
              {cleaning ? (
                <>
                  <Loader2 className="animate-spin h-5 w-5 mr-2" />
                  Cleaning...
                </>
              ) : (
                <>
                  <Sparkles className="h-5 w-5 mr-2" />
                  Start Cleaning
                </>
              )}
            </button>
          </form>
        </div>

        {cleanError && (
          <div className="bg-white border-2 border-black text-black px-4 py-3 rounded-lg flex items-center mt-4">
            <XCircle className="h-5 w-5 mr-2" />
            {cleanError}
          </div>
        )}

        {cleanResult && (
          <div className="bg-white rounded-lg shadow p-6 mt-4">
            <h3 className="text-xl font-bold text-gray-900 mb-4">Cleaning Result</h3>
            {cleanResult.job_id && (
              <div className="mb-4 p-4 bg-gray-50 border border-gray-300 rounded-lg">
                <p className="text-sm text-black">
                  <strong>Job ID:</strong> {cleanResult.job_id}
                </p>
                <p className="text-sm text-black">
                  <strong>Status:</strong> {cleanResult.status}
                </p>
              </div>
            )}
            {cleanResult.result?.summary && (
              <div className="mb-4 p-4 bg-gray-50 border border-gray-300 rounded-lg">
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div>
                    <p className="text-sm text-gray-600">Total Entries</p>
                    <p className="text-2xl font-bold text-black">
                      {cleanResult.result.summary.totalEntries || 0}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">Kept</p>
                    <p className="text-2xl font-bold text-black">
                      {cleanResult.result.summary.kept || 0}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">Duplicates Removed</p>
                    <p className="text-2xl font-bold text-gray-700">
                      {cleanResult.result.summary.removed_duplicate_url || 0}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">No Date Removed</p>
                    <p className="text-2xl font-bold text-gray-800">
                      {cleanResult.result.summary.removed_no_date || 0}
                    </p>
                  </div>
                </div>
              </div>
            )}
            <pre className="bg-gray-50 p-4 rounded-lg overflow-auto text-sm">
              {JSON.stringify(cleanResult, null, 2)}
            </pre>
          </div>
        )}
      </div>

      {/* Download Buttons */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Download Files</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <button
            onClick={() => handleDownload('articles')}
            className="flex items-center justify-center px-4 py-3 bg-black text-white rounded-lg hover:bg-gray-800 border border-black"
          >
            <Download className="h-5 w-5 mr-2" />
            Download Articles
          </button>
          <button
            onClick={() => handleDownload('cleaned')}
            className="flex items-center justify-center px-4 py-3 bg-black text-white rounded-lg hover:bg-gray-800 border border-black"
          >
            <Download className="h-5 w-5 mr-2" />
            Download Cleaned
          </button>
          <button
            onClick={() => handleDownload('selectors')}
            className="flex items-center justify-center px-4 py-3 bg-black text-white rounded-lg hover:bg-gray-800 border border-black"
          >
            <Download className="h-5 w-5 mr-2" />
            Download Selectors
          </button>
        </div>
      </div>
    </div>
  )
}

export default ScrapePage

