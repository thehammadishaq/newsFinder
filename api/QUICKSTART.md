# Quick Start Guide
# Purana venv delete karein (optional)
Remove-Item -Recurse -Force .venv

# Naya venv create karein
python -m venv .venv

# Activate karein
.\.venv\Scripts\Activate.ps1

# Dependencies install karein
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m playwright install
## 1. Install Dependencies

```bash
pip install -r requirements.txt
python -m playwright install
```

## 2. Set Up Environment Variables

Create a `.env` file in the project root:

```env
DEEPSEEK_API_KEY=sk-your-key-here
# OR
OPENAI_API_KEY=sk-your-key-here
```

## 3. Start the API Server

```bash
# Option 1: Using the run script
python run_api.py

# Option 2: Using uvicorn directly
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at:
- **API**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 4. Test the API

### Using Python requests:

```python
import requests

# 1. Discover selectors
response = requests.post(
    "http://localhost:8000/api/v1/discover/sync",
    json={
        "urls": ["https://www.example.com"],
        "recent_hours": 24,
        "site_concurrency": 1,
    }
)
print(response.json())

# 2. Check status
status = requests.get("http://localhost:8000/api/v1/status").json()
print(status)
```

### Using cURL:

```bash
# Health check
curl http://localhost:8000/health

# Discover selectors
curl -X POST "http://localhost:8000/api/v1/discover/sync" \
  -H "Content-Type: application/json" \
  -d '{
    "urls": ["https://www.example.com"],
    "recent_hours": 24
  }'
```

## 5. Complete Workflow Example

```python
import requests
import time

BASE_URL = "http://localhost:8000"

# Step 1: Discover selectors (async)
discover_response = requests.post(
    f"{BASE_URL}/api/v1/discover",
    json={
        "urls": ["https://www.example.com"],
        "recent_hours": 24,
        "site_concurrency": 1,
    }
)
job_id = discover_response.json()["job_id"]
print(f"Discovery job started: {job_id}")

# Step 2: Wait for completion
while True:
    status = requests.get(f"{BASE_URL}/api/v1/status/{job_id}").json()
    if status["status"] == "completed":
        print("Discovery completed!")
        break
    elif status["status"] == "failed":
        print(f"Discovery failed: {status.get('error')}")
        break
    time.sleep(2)

# Step 3: Scrape articles
scrape_response = requests.post(
    f"{BASE_URL}/api/v1/scrape",
    json={
        "mode": "auto",
        "site_concurrency": 2,
    }
)
scrape_job_id = scrape_response.json()["job_id"]
print(f"Scraping job started: {scrape_job_id}")

# Step 4: Wait for scraping
while True:
    status = requests.get(f"{BASE_URL}/api/v1/status/{scrape_job_id}").json()
    if status["status"] == "completed":
        print("Scraping completed!")
        break
    time.sleep(2)

# Step 5: Clean articles
clean_response = requests.post(
    f"{BASE_URL}/api/v1/clean",
    json={}
)
clean_job_id = clean_response.json()["job_id"]
print(f"Cleaning job started: {clean_job_id}")

# Step 6: Download cleaned articles
articles = requests.get(f"{BASE_URL}/api/v1/download/cleaned")
with open("articles.jsonl", "wb") as f:
    f.write(articles.content)
print("Articles downloaded!")
```

## Common Issues

### Port already in use
```bash
# Use a different port
uvicorn api.main:app --port 8001
```

### Module not found
```bash
# Make sure you're in the project root directory
cd /path/to/newsFinder
python run_api.py
```

### Playwright browsers not installed
```bash
python -m playwright install
```

## Production Deployment

For production, use:

```bash
uvicorn api.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 4 \
  --log-level info
```

Or use a process manager like `systemd`, `supervisor`, or `pm2`.

