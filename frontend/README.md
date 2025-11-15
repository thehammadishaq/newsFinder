# News Scraper Frontend

React + Vite frontend for the News Scraper API.

## Installation

```bash
cd frontend
npm install
```

## Development

```bash
npm run dev
```

The app will be available at http://localhost:3000

## Build

```bash
npm run build
```

## Features

- **Dashboard**: Overview of pipeline status and statistics
- **Discover**: Find sitemap and CSS selectors for news sites
- **Scrape**: Extract articles using discovered selectors
- **Clean**: Filter and deduplicate scraped articles
- **Status**: Monitor jobs and site status

## Environment Variables

Create a `.env` file in the frontend directory (optional):

```env
VITE_API_URL=http://localhost:9001
```

## API Integration

The frontend connects to the FastAPI backend running on port 9001 by default. Make sure the backend is running before using the frontend.

