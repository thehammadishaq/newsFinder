"""
FastAPI application for News Scraper
"""
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, File, UploadFile
from fastapi import Path as PathParam
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import os
import json
from datetime import datetime
import asyncio
from pathlib import Path

from api.models import (
    SelectorDiscoveryRequest,
    SelectorDiscoveryResponse,
    ScrapingRequest,
    ScrapingResponse,
    CleaningRequest,
    CleaningResponse,
    StatusResponse,
    SiteStatus,
    JobStatus,
)
from api.services import (
    SelectionService,
    ScrapingService,
    CleaningService,
    StatusService,
)

app = FastAPI(
    title="News Scraper API",
    description="API for discovering selectors and scraping news articles",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
selection_service = SelectionService()
scraping_service = ScrapingService()
cleaning_service = CleaningService()
status_service = StatusService()

# Job tracking (in-memory, consider Redis for production)
job_status: Dict[str, JobStatus] = {}


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "News Scraper API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "discover": "/api/v1/discover",
            "scrape": "/api/v1/scrape",
            "clean": "/api/v1/clean",
            "status": "/api/v1/status",
            "docs": "/docs",
        },
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ============================================================================
# SELECTOR DISCOVERY ENDPOINTS
# ============================================================================

@app.post("/api/v1/discover", response_model=SelectorDiscoveryResponse)
async def discover_selectors(
    request: SelectorDiscoveryRequest,
    background_tasks: BackgroundTasks,
):
    """
    Discover selectors (sitemap/CSS) for news sites.
    
    This endpoint starts a background job to discover selectors.
    Use the status endpoint to check progress.
    """
    job_id = f"discover_{datetime.utcnow().timestamp()}"
    
    # Initialize job status
    job_status[job_id] = JobStatus(
        job_id=job_id,
        type="discover",
        status="running",
        created_at=datetime.utcnow(),
        progress=0,
    )
    
    # Start background task
    background_tasks.add_task(
        _run_discovery,
        job_id=job_id,
        request=request,
    )
    
    return SelectorDiscoveryResponse(
        job_id=job_id,
        message="Selector discovery started",
        status="running",
    )


async def _run_discovery(job_id: str, request: SelectorDiscoveryRequest):
    """Background task for selector discovery"""
    try:
        job_status[job_id].status = "running"
        job_status[job_id].progress = 10
        
        result = await selection_service.discover_selectors(
            urls=request.urls,
            recent_hours=request.recent_hours,
            site_concurrency=request.site_concurrency,
            llm_concurrency=request.llm_concurrency,
            timeout=request.timeout,
            max_depth=request.max_depth,
        )
        
        job_status[job_id].status = "completed"
        job_status[job_id].progress = 100
        job_status[job_id].result = result
        job_status[job_id].completed_at = datetime.utcnow()
        
    except Exception as e:
        job_status[job_id].status = "failed"
        job_status[job_id].error = str(e)
        job_status[job_id].completed_at = datetime.utcnow()


@app.post("/api/v1/discover/sync", response_model=SelectorDiscoveryResponse)
async def discover_selectors_sync(request: SelectorDiscoveryRequest):
    """
    Synchronously discover selectors (use for small batches or testing).
    
    For large batches, use /api/v1/discover instead.
    """
    try:
        result = await selection_service.discover_selectors(
            urls=request.urls,
            recent_hours=request.recent_hours,
            site_concurrency=request.site_concurrency,
            llm_concurrency=request.llm_concurrency,
            timeout=request.timeout,
            max_depth=request.max_depth,
        )
        
        return SelectorDiscoveryResponse(
            job_id=f"sync_{datetime.utcnow().timestamp()}",
            message="Selector discovery completed",
            status="completed",
            result=result,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# SCRAPING ENDPOINTS
# ============================================================================

@app.post("/api/v1/scrape", response_model=ScrapingResponse)
async def scrape_articles(
    request: ScrapingRequest,
    background_tasks: BackgroundTasks,
):
    """
    Scrape articles using discovered selectors.
    
    This endpoint starts a background job to scrape articles.
    Use the status endpoint to check progress.
    """
    job_id = f"scrape_{datetime.utcnow().timestamp()}"
    
    job_status[job_id] = JobStatus(
        job_id=job_id,
        type="scrape",
        status="running",
        created_at=datetime.utcnow(),
        progress=0,
    )
    
    background_tasks.add_task(
        _run_scraping,
        job_id=job_id,
        request=request,
    )
    
    return ScrapingResponse(
        job_id=job_id,
        message="Scraping started",
        status="running",
    )


async def _run_scraping(job_id: str, request: ScrapingRequest):
    """Background task for scraping"""
    try:
        job_status[job_id].status = "running"
        job_status[job_id].progress = 10
        
        result = await scraping_service.scrape_articles(
            stream_path=request.stream_path,
            mode=request.mode,
            site_concurrency=request.site_concurrency,
            target_concurrency=request.target_concurrency,
            timeout=request.timeout,
            max_items=request.max_items,
        )
        
        job_status[job_id].status = "completed"
        job_status[job_id].progress = 100
        job_status[job_id].result = result
        job_status[job_id].completed_at = datetime.utcnow()
        
    except Exception as e:
        job_status[job_id].status = "failed"
        job_status[job_id].error = str(e)
        job_status[job_id].completed_at = datetime.utcnow()


@app.post("/api/v1/scrape/sync", response_model=ScrapingResponse)
async def scrape_articles_sync(request: ScrapingRequest):
    """Synchronously scrape articles"""
    try:
        result = await scraping_service.scrape_articles(
            stream_path=request.stream_path,
            mode=request.mode,
            site_concurrency=request.site_concurrency,
            target_concurrency=request.target_concurrency,
            timeout=request.timeout,
            max_items=request.max_items,
        )
        
        return ScrapingResponse(
            job_id=f"sync_{datetime.utcnow().timestamp()}",
            message="Scraping completed",
            status="completed",
            result=result,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# CLEANING ENDPOINTS
# ============================================================================

@app.post("/api/v1/clean", response_model=CleaningResponse)
async def clean_articles(
    request: CleaningRequest,
    background_tasks: BackgroundTasks,
):
    """Clean and filter scraped articles"""
    job_id = f"clean_{datetime.utcnow().timestamp()}"
    
    job_status[job_id] = JobStatus(
        job_id=job_id,
        type="clean",
        status="running",
        created_at=datetime.utcnow(),
        progress=0,
    )
    
    background_tasks.add_task(
        _run_cleaning,
        job_id=job_id,
        request=request,
    )
    
    return CleaningResponse(
        job_id=job_id,
        message="Cleaning started",
        status="running",
    )


async def _run_cleaning(job_id: str, request: CleaningRequest):
    """Background task for cleaning"""
    try:
        job_status[job_id].status = "running"
        job_status[job_id].progress = 10
        
        result = await cleaning_service.clean_articles(
            input_path=request.input_path,
        )
        
        job_status[job_id].status = "completed"
        job_status[job_id].progress = 100
        job_status[job_id].result = result
        job_status[job_id].completed_at = datetime.utcnow()
        
    except Exception as e:
        job_status[job_id].status = "failed"
        job_status[job_id].error = str(e)
        job_status[job_id].completed_at = datetime.utcnow()


@app.post("/api/v1/clean/sync", response_model=CleaningResponse)
async def clean_articles_sync(request: CleaningRequest):
    """Synchronously clean articles"""
    try:
        result = await cleaning_service.clean_articles(
            input_path=request.input_path,
        )
        
        return CleaningResponse(
            job_id=f"sync_{datetime.utcnow().timestamp()}",
            message="Cleaning completed",
            status="completed",
            result=result,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# STATUS & MONITORING ENDPOINTS
# ============================================================================

@app.get("/api/v1/status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Get status of a specific job"""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_status[job_id]


@app.get("/api/v1/status")
async def get_overall_status(
    domain: Optional[str] = Query(None, description="Filter by domain"),
    limit: int = Query(100, ge=1, le=1000, description="Limit results"),
):
    """Get overall pipeline status"""
    try:
        result = await status_service.get_status(domain=domain, limit=limit)
        # Ensure datetime is serializable
        if isinstance(result.get("last_updated"), datetime):
            result["last_updated"] = result["last_updated"].isoformat()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")


@app.get("/api/v1/sites")
async def get_sites_status(
    domain: Optional[str] = Query(None, description="Filter by domain"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get status of all sites"""
    try:
        return await status_service.get_sites_status(domain=domain, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting sites status: {str(e)}")


# ============================================================================
# FILE UPLOAD/DOWNLOAD ENDPOINTS
# ============================================================================

@app.post("/api/v1/upload/urls")
async def upload_urls_file(file: UploadFile = File(...)):
    """Upload Excel file with URLs for batch processing"""
    try:
        # Save uploaded file
        upload_dir = Path("uploads")
        upload_dir.mkdir(exist_ok=True)
        
        file_path = upload_dir / f"{datetime.utcnow().timestamp()}_{file.filename}"
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        return {
            "message": "File uploaded successfully",
            "file_path": str(file_path),
            "filename": file.filename,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/download/{file_type}")
async def download_file(
    file_type: str = PathParam(..., description="Type: selectors, articles, cleaned, overview"),
):
    """Download output files"""
    file_map = {
        "selectors": "selection_extraction_report_stream.jsonl",
        "articles": "stream_scraped_articles.jsonl",
        "cleaned": "articles_clean_current.jsonl",
        "overview": "pipelines_overview.csv",
    }
    
    if file_type not in file_map:
        raise HTTPException(status_code=400, detail="Invalid file type")
    
    file_path = Path(file_map[file_type])
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type="application/octet-stream",
    )


# ============================================================================
# UTILITY ENDPOINTS
# ============================================================================

@app.get("/api/v1/jobs", response_model=List[JobStatus])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=500),
):
    """List all jobs"""
    jobs = list(job_status.values())
    
    if status:
        jobs = [j for j in jobs if j.status == status]
    
    # Sort by created_at descending
    jobs.sort(key=lambda x: x.created_at, reverse=True)
    
    return jobs[:limit]


@app.delete("/api/v1/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job from tracking"""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    del job_status[job_id]
    return {"message": "Job deleted", "job_id": job_id}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

