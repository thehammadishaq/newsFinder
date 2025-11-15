# News Scraper FastAPI

FastAPI application for the news scraping pipeline.

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
python -m playwright install
```

## Running the API

```bash
# Development server
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# Production server
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Documentation

Once the server is running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Main Endpoints

### 1. Discover Selectors

Discover sitemap and CSS selectors for news sites.

**POST** `/api/v1/discover`
- Starts a background job
- Returns job_id for status tracking

**POST** `/api/v1/discover/sync`
- Synchronous discovery (for small batches)

**Request Body:**
```json
{
  "urls": ["https://www.example.com", "https://www.news.com"],
  "recent_hours": 24,
  "site_concurrency": 2,
  "llm_concurrency": 3,
  "timeout": 15.0,
  "max_depth": 2
}
```

### 2. Scrape Articles

Scrape articles using discovered selectors.

**POST** `/api/v1/scrape`
- Starts a background job

**POST** `/api/v1/scrape/sync`
- Synchronous scraping

**Request Body:**
```json
{
  "stream_path": "selection_extraction_report_stream.jsonl",
  "mode": "auto",
  "site_concurrency": 2,
  "target_concurrency": 6,
  "timeout": 15.0,
  "max_items": 500
}
```

### 3. Clean Articles

Clean and filter scraped articles.

**POST** `/api/v1/clean`
- Starts a background job

**POST** `/api/v1/clean/sync`
- Synchronous cleaning

**Request Body:**
```json
{
  "input_path": "stream_scraped_articles.jsonl"
}
```

### 4. Status & Monitoring

**GET** `/api/v1/status/{job_id}`
- Get status of a specific job

**GET** `/api/v1/status`
- Get overall pipeline status
- Query params: `domain`, `limit`

**GET** `/api/v1/sites`
- Get status of all sites
- Query params: `domain`, `limit`

**GET** `/api/v1/jobs`
- List all jobs
- Query params: `status`, `limit`

### 5. File Operations

**POST** `/api/v1/upload/urls`
- Upload Excel file with URLs for batch processing

**GET** `/api/v1/download/{file_type}`
- Download output files
- Types: `selectors`, `articles`, `cleaned`, `overview`

## Example Usage

### Python Client

```python
import requests

# Discover selectors
response = requests.post(
    "http://localhost:8000/api/v1/discover",
    json={
        "urls": ["https://www.example.com"],
        "recent_hours": 24,
        "site_concurrency": 1,
    }
)
job_id = response.json()["job_id"]

# Check status
status = requests.get(f"http://localhost:8000/api/v1/status/{job_id}").json()
print(status)

# Scrape articles
scrape_response = requests.post(
    "http://localhost:8000/api/v1/scrape",
    json={
        "mode": "auto",
        "site_concurrency": 2,
    }
)

# Clean articles
clean_response = requests.post(
    "http://localhost:8000/api/v1/clean",
    json={}
)
```

### cURL Examples

```bash
# Discover selectors
curl -X POST "http://localhost:8000/api/v1/discover" \
  -H "Content-Type: application/json" \
  -d '{
    "urls": ["https://www.example.com"],
    "recent_hours": 24
  }'

# Check job status
curl "http://localhost:8000/api/v1/status/{job_id}"

# Get overall status
curl "http://localhost:8000/api/v1/status"

# Download cleaned articles
curl "http://localhost:8000/api/v1/download/cleaned" -o articles.jsonl
```

## Environment Variables

Create a `.env` file:

```env
DEEPSEEK_API_KEY=sk-your-key
OPENAI_API_KEY=sk-your-key
PROXY_SERVER=http://proxy:port  # Optional
```

## Architecture

```
api/
├── main.py          # FastAPI application
├── models.py        # Pydantic models
├── services.py      # Service layer wrapping pipelines
└── __init__.py
```

The API wraps the existing pipeline modules:
- `selection_extraction_pipeline.py` - Selector discovery
- `stream_scraping_pipeline.py` - Article scraping
- `clean_selection_entries.py` - Article cleaning

## Background Jobs

Long-running operations run as background tasks. Use the job_id to track progress:

1. Start a job (returns job_id)
2. Poll `/api/v1/status/{job_id}` for updates
3. Check `status` field: `running`, `completed`, `failed`
4. When `completed`, check `result` field for output

## Error Handling

All endpoints return appropriate HTTP status codes:
- `200` - Success
- `400` - Bad Request
- `404` - Not Found
- `500` - Internal Server Error

Error responses include a `detail` field with error message.

