python -m venv .venv
.\.venv\Scripts\Activate



pip install -r requirements.txt
python -m playwright install

Option 1: Use the helper script (recommended)
bash install_playwright_deps.sh

Option 2: Run the command directly
python3 -m playwright install-deps

==================================


for activating localEnvoirment 
.\.venv\Scripts\Activate.ps1

 source .venv/bin/activate

===================================

.env support (recommended)

Create a file named .env in python_tools with your keys:

DEEPSEEK_API_KEY=sk-your-deepseek-key
# Optional overrides
# DEEPSEEK_BASE_URL=https://api.deepseek.com
# DEEPSEEK_MODEL=deepseek-chat
# OPENAI_API_KEY=sk-openai...
# OPENAI_BASE_URL=https://api.openai.com
# OPENAI_MODEL=gpt-4o-mini

Alternatively, set once in PowerShell (persists for future sessions):
setx DEEPSEEK_API_KEY "sk-your-deepseek-key"

===================

run a selector extractor

python selector_scraper.py https://www.tradingview.com/news/ --headful --slowmo 150 --output selectors.json



==================================================

python news_scraper_via_selectors.py https://www.tradingview.com/news/ --selectors python_tools/selectors.json --headful --slowmo 150 --output python_tools\articles.json

===================================

Excel batch mode

# Put URLs in urls.xlsx (first row header 'url' or first column)
# Example run (from python_tools):
python selector_scraper.py --excel urls.xlsx --headful --slowmo 150 --output selectors_combined.json

# Outputs:
# - selectors_<domain>_<timestamp>.json per URL
# - selectors_combined.json with summary and per-URL paths

===================================

Commands (quick reference)

# 1) Setup (run once)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install

# 2) .env (recommended)
# Create python_tools/.env with keys (see above)

# 3) Discover selectors (single URL)
python selector_scraper.py https://example.com/news --output selectors.json
# Headful debug
python selector_scraper.py https://example.com/news --headful --slowmo 150 --output selectors.json

# 4) Discover selectors (Excel batch)
python selector_scraper.py --excel urls.xlsx --concurrency 4 --output selectors_combined.json
# Serial headful (to watch browser)
python selector_scraper.py --excel urls.xlsx --concurrency 1 --headful --slowmo 150 --output selectors_combined.json

# 5) Extract articles (single selectors)
python news_scraper_via_selectors.py https://example.com/news --selectors selectors.json --output articles.json
# Headful debug
python news_scraper_via_selectors.py https://example.com/news --selectors selectors.json --headful --slowmo 150 --output articles.json

# 6) Extract articles (combined selectors from Excel)
# PowerShell single-line:
python news_scraper_via_selectors.py https://example.com/news --selectors selectors_combined.json --for-url https://example.com/news --output articles.json
# Or PowerShell multiline with backticks:
python news_scraper_via_selectors.py https://example.com/news `
  --selectors selectors_combined.json `
  --for-url https://example.com/news `
  --output articles.json
# You can also pass just the domain to --for-url (e.g., example.com)

# 7) Batch extract ALL sites from combined (parallel)
# Note: positional URL is required but ignored in this mode; you can pass any placeholder
python news_scraper_via_selectors.py https://placeholder --selectors selectors_combined.json --concurrency 4 --output articles_combined.json
# Or PowerShell multiline with backticks:
python news_scraper_via_selectors.py https://placeholder `
  --selectors selectors_combined.json `
  --concurrency 4 `
  --output articles_combined.json
  
# Per-site outputs will be saved as articles_<domain>_<timestamp>.json; combined summary at articles_combined.json

# To watch the browser, use headful with single worker:
python news_scraper_via_selectors.py https://placeholder --selectors selectors_combined.json --concurrency 1 --headful --slowmo 150 --output articles_combined.json
# Or PowerShell multiline with backticks:
python news_scraper_via_selectors.py https://placeholder `
  --selectors selectors_combined.json `
  --concurrency 1 `
  --headful `
  --slowmo 150 `
  --output articles_combined.json

===================================

Auto pipeline (sitemap first, selectors fallback)

# Primary behavior:
# 1) Find ALL sitemaps listed in robots.txt, expand them all, then filter entries updated in the last N hours (default 24) using publication_date/lastmod.
#    If some entries found, write them to output.
# 2) If sitemap has no entries, discover selectors on the given URL and extract articles using those selectors.

# Example (writes combined output to pipeline_output.json):
python auto_selectors_pipeline.py https://www.example.com --output python_tools\pipeline_output.json

# Change recent window (e.g., last 6 hours):
python auto_selectors_pipeline.py https://www.example.com --recent-hours 6 --output python_tools\pipeline_output.json

# Optional:
# - Use headful browser and slowmo when fallback triggers selectors:
python auto_selectors_pipeline.py https://www.example.com --headful --slowmo 150 --output python_tools\pipeline_output.json

# Debug artifacts:
# - debug_html/<domain>_*_SITEMAP_ENTRIES.json : full sitemap entries captured

Excel batch mode (auto pipeline)

# Put URLs in urls.xlsx (first row header 'url' or first column)
# Example run:
python auto_selectors_pipeline.py --excel python_tools\urls.xlsx --concurrency 4 --recent-hours 24 --output python_tools\pipeline_output.json

# Output: combined JSON with per-site results and aggregated articles

===================================

Two-pipeline flow

1) Searching pipeline (build selectors only)

# Single URL: produce sitemap and/or CSS selectors without scraping data
python searching_pipeline.py https://www.example.com --recent-hours 24 --output python_tools\selectors_search.json

# Excel batch:
python searching_pipeline.py --excel python_tools\urls.xlsx --concurrency 4 --recent-hours 24 --output python_tools\selectors_search.json

# Output shape:
# { success, mode: "searching", entries: [ { url, domain, sitemap?: {present, sitemaps:[{url, likelyRecent}]}, css?: {present, sections:[...] } } ] }

2) Scraping pipeline (use selectors to fetch data)

# Single URL with selectors from step 1
python scraping_pipeline.py https://www.example.com --selectors python_tools\selectors_search.json --output python_tools\scrape_output.json

# Uses CSS selectors if present; sitemap selectors reserved for future article-level extraction

===================================















python auto_selectors_pipeline.py https://www.investopedia.com --output python_tools\pipeline_output.json


python auto_selectors_pipeline.py --excel python_tools\urls.xlsx --concurrency 4 --recent-hours 24 --output python_tools\pipeline_output.json



python searching_pipeline.py --excel python_tools\urls.xlsx --recent-hours 24 --output python_tools\selectors_search.json



python searching_pipeline.py --excel python_tools\urls.xlsx --export-like --sitemaps-only --output python_tools\selectors_search.json

python scraping_pipeline.py https://investopedia.com --selectors selectors_search.json --mode sitemap --recent-hours 24 --output python_tools\scrape_investopedia.json



python scraping_pipeline.py https://investopedia.com --selectors python_tools\selectors_search.json --mode sitemap --recent-hours 24 --output python_tools\scrape_investopedia.json




python scraping_pipeline.py --selectors python_tools\selectors_search.json --batch --mode auto --recent-hours 24 --output python_tools\scrape_all.json







python searching_pipeline.py --excel python_tools\urls.xlsx --recent-hours 24 --output python_tools\selectors_search.json



python scraping_pipeline.py --selectors python_tools\selectors_search.json --batch --mode auto --recent-hours 24 --output python_tools\scrape_all.json





========================================
python searching_pipeline.py --excel python_tools\urls.xlsx --concurrency 4 --recent-hours 24 --llm-filter --output python_tools\selectors_search.json

python scraping_pipeline.py --selectors python_tools\selectors_search.json --batch --mode auto --concurrency 4 --recent-hours 24 --output python_tools\scrape_all.json





=====================================================================================================================================================

python searching_pipeline.py --excel urls.xlsx --concurrency 10 --recent-hours 24 --output selectors_search.json

python scraping_pipeline.py --selectors python_tools\selectors_search.json --batch --mode auto --concurrency 4 --recent-hours 24 --output python_tools\scrape_all.json



===========================================================================================================================

python selection_extraction_pipeline.py

python selection_extraction_pipeline.py --site-concurrency 500 --llm-concurrency 3 --recent-hours 24 --timeout 15 --max-depth 2


python stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output stream_scraped_articles.jsonl --mode auto --concurrency 2

python stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output stream_scraped_articles.jsonl --mode auto


python3 stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output stream_scraped_articles.jsonl --mode auto --site-concurrency 3 --target-concurrency 6 --sitemap-concurrency 12 --css-concurrency 1 --http-concurrency 24 --per-domain-cap 1 --max 500

=====================================================================

python stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output python_tools/stream_scraped_articles.jsonl --mode auto

python stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output python_tools/stream_scraped_articles.jsonl --mode auto --once

python stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output stream_scraped_articles.jsonl --mode auto

=================================================================

Main pipline commands


python selection_extraction_pipeline.py --site-concurrency 2 --llm-concurrency 3 --recent-hours 24 --timeout 15 --max-depth 2

python stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output stream_scraped_articles.jsonl --mode auto --site-concurrency 2 --target-concurrency 6 --sitemap-concurrency 12 --css-concurrency 1 --http-concurrency 24 --per-domain-cap 1 --max 500 --once

=============================================================================================
Commands for logging outputs
=============================================================================================

python selection_extraction_pipeline.py --site-concurrency 2 --llm-concurrency 5
python selection_extraction_pipeline.py --site-concurrency 1 --timeout 30
python selection_extraction_pipeline.py --site-concurrency 1 --timeout 60.0

python stream_scraping_pipeline.py --site-concurrency 2 

python stream_scraping_pipeline.py --sites-xlsx .\scrapeURLs.xlsx --site-concurrency 2

python stream_scraping_pipeline.py --site-concurrency 2 --targets-json selection_extraction_targets.json

python clean_selection_entries.py


=============================================================================================
Commands for logging outputs
=============================================================================================

python selection_extraction_pipeline.py --site-concurrency 1 2>&1 | Tee-Object -FilePath run.log -Append

Start-Transcript -Path run.log
python selection_extraction_pipeline.py --site-concurrency 1
Stop-Transcript

=============================================================================================
Commands for Selector Extracion Aggregator outputs
=============================================================================================

1. One stream file, write outputs to outputs/
python aggregate_selection_artifacts.py --streams selection_extraction_report_stream.jsonl --out-dir outputs

2. Multiple explicit stream files
python aggregate_selection_artifacts.py --streams run1.jsonl --streams run2.jsonl --streams run3.jsonl --out-dir outputs

3. Glob all matching stream files
python aggregate_selection_artifacts.py --streams-glob "selection_extraction_report_stream*.jsonl" --out-dir outputs

4. Write files without timestamp suffix (use “latest” names)
python aggregate_selection_artifacts.py --streams-glob "selection_extraction_report_stream*.jsonl" --out-dir outputs --no-ts-suffix


=============================================================================================
Commands for Scraper outputs
=============================================================================================
Use one targets file at a time:

python stream_scraping_pipeline.py --once --targets-json outputs/selection_targets_sitemap_YYYYMMDD.json
python stream_scraping_pipeline.py --once --targets-json outputs/selection_targets_css_YYYYMMDD.json




=============================================================================================
Commands for Fast Scraping
=============================================================================================
python stream_scraping_pipeline.py --stream selection_extraction_report_stream.jsonl --output stream_scraped_articles.jsonl --site-concurrency 2  --target-concurrency 50 --sitemap-concurrency 50 --css-concurrency 0  --http-concurrency 50  --per-domain-cap 50  --timeout 60.0