import { useState } from 'react'
import { Sparkles, Loader2, XCircle, Download, CheckCircle } from 'lucide-react'
import apiService from '../services/api'

function CleanPage() {
  const [inputPath, setInputPath] = useState('')
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
        input_path: inputPath || undefined,
      }

      const response = useSync
        ? await apiService.cleanArticlesSync(data)
        : await apiService.cleanArticles(data)

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

  const handleDownload = async () => {
    try {
      const blob = await apiService.downloadFile('cleaned')
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = 'cleaned_articles.jsonl'
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
        <h1 className="text-3xl font-bold text-gray-900">Clean Articles</h1>
        <p className="mt-2 text-gray-600">Filter, deduplicate, and clean scraped articles</p>
      </div>

      <div className="bg-white rounded-lg shadow p-6">
        <form onSubmit={handleSubmit} className="space-y-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Input Path (optional)
            </label>
            <input
              type="text"
              value={inputPath}
              onChange={(e) => setInputPath(e.target.value)}
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

      {/* Download Button */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Download Cleaned Articles</h2>
        <button
          onClick={handleDownload}
          className="flex items-center justify-center px-6 py-3 bg-black text-white rounded-lg hover:bg-gray-800 border border-black"
        >
          <Download className="h-5 w-5 mr-2" />
          Download Cleaned Articles
        </button>
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
          {result.result?.summary && (
            <div className="mb-4 p-4 bg-gray-50 border border-gray-300 rounded-lg">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div>
                  <p className="text-sm text-gray-600">Total Entries</p>
                  <p className="text-2xl font-bold text-black">
                    {result.result.summary.totalEntries || 0}
                  </p>
                </div>
                <div>
                  <p className="text-sm text-gray-600">Kept</p>
                  <p className="text-2xl font-bold text-black">
                    {result.result.summary.kept || 0}
                  </p>
                </div>
                <div>
                  <p className="text-sm text-gray-600">Duplicates Removed</p>
                  <p className="text-2xl font-bold text-gray-700">
                    {result.result.summary.removed_duplicate_url || 0}
                  </p>
                </div>
                <div>
                  <p className="text-sm text-gray-600">No Date Removed</p>
                  <p className="text-2xl font-bold text-gray-800">
                    {result.result.summary.removed_no_date || 0}
                  </p>
                </div>
              </div>
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

export default CleanPage

