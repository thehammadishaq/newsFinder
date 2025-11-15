import { useState } from 'react'
import { FileText, Loader2, CheckCircle, XCircle, Download } from 'lucide-react'
import apiService from '../services/api'

function ScrapePage() {
  const [streamPath, setStreamPath] = useState('')
  const [mode, setMode] = useState('auto')
  const [siteConcurrency, setSiteConcurrency] = useState(1)
  const [targetConcurrency, setTargetConcurrency] = useState(6)
  const [timeout, setTimeout] = useState(15.0)
  const [maxItems, setMaxItems] = useState(500)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [useSync, setUseSync] = useState(false)

  const handleSubmit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError(null)
    setResult(null)

    try {
      const data = {
        stream_path: streamPath || undefined,
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
        const status = await apiService.getJobStatus(jobId)
        if (status.status === 'completed' || status.status === 'failed') {
          clearInterval(interval)
          setResult(status)
          setLoading(false)
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

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Scrape Articles</h1>
        <p className="mt-2 text-gray-600">Extract articles using discovered selectors</p>
      </div>

      <div className="bg-white rounded-lg shadow p-6">
        <form onSubmit={handleSubmit} className="space-y-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Stream Path (optional)
            </label>
            <input
              type="text"
              value={streamPath}
              onChange={(e) => setStreamPath(e.target.value)}
              placeholder="selection_extraction_report_stream.jsonl"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500"
            />
            <p className="mt-1 text-sm text-gray-500">
              Leave empty to use default stream file
            </p>
          </div>

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
                onChange={(e) => setSiteConcurrency(parseInt(e.target.value))}
                min="1"
                max="10"
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
                onChange={(e) => setTargetConcurrency(parseInt(e.target.value))}
                min="1"
                max="50"
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
                Max Items
              </label>
              <input
                type="number"
                value={maxItems}
                onChange={(e) => setMaxItems(parseInt(e.target.value))}
                min="1"
                max="10000"
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

      {error && (
        <div className="bg-white border-2 border-black text-black px-4 py-3 rounded-lg flex items-center">
          <XCircle className="h-5 w-5 mr-2" />
          {error}
        </div>
      )}

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
    </div>
  )
}

export default ScrapePage

