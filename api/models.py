"""
Pydantic models for API requests and responses
"""
from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Dict, Any, Literal
from datetime import datetime


# ============================================================================
# REQUEST MODELS
# ============================================================================

class SelectorDiscoveryRequest(BaseModel):
    """Request model for selector discovery"""
    urls: List[str] = Field(..., description="List of URLs to discover selectors for")
    recent_hours: int = Field(24, ge=1, le=168, description="Recent hours threshold for filtering")
    site_concurrency: int = Field(1, ge=1, le=10, description="Number of sites to process in parallel")
    llm_concurrency: int = Field(3, ge=1, le=10, description="Number of LLM detections per site in parallel")
    timeout: float = Field(15.0, ge=1.0, le=120.0, description="Per-request timeout in seconds")
    max_depth: int = Field(2, ge=1, le=5, description="Max sitemap recursion depth")
    
    class Config:
        json_schema_extra = {
            "example": {
                "urls": ["https://www.example.com", "https://www.news.com"],
                "recent_hours": 24,
                "site_concurrency": 2,
                "llm_concurrency": 3,
                "timeout": 15.0,
                "max_depth": 2,
            }
        }


class ScrapingRequest(BaseModel):
    """Request model for article scraping"""
    stream_path: Optional[str] = Field(
        None,
        description="Path to selection_extraction_report_stream.jsonl (default: auto-detect)"
    )
    targets_json: Optional[str] = Field(
        None,
        description="Path to selection_extraction_targets.json (alternative to stream_path)"
    )
    mode: Literal["auto", "sitemap", "css", "both"] = Field(
        "auto",
        description="Scraping mode"
    )
    site_concurrency: int = Field(1, ge=1, description="Site-level concurrency")
    target_concurrency: int = Field(6, ge=1, description="Target-level concurrency")
    timeout: float = Field(15.0, ge=0.1, description="Per-request timeout")
    max_items: int = Field(500, ge=1, description="Max items per source")
    
    class Config:
        json_schema_extra = {
            "example": {
                "stream_path": "selection_extraction_report_stream.jsonl",
                "mode": "auto",
                "site_concurrency": 2,
                "target_concurrency": 6,
                "timeout": 15.0,
                "max_items": 500,
            }
        }


class CleaningRequest(BaseModel):
    """Request model for article cleaning"""
    input_path: Optional[str] = Field(
        None,
        description="Path to stream_scraped_articles.jsonl (default: auto-detect)"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "input_path": "stream_scraped_articles.jsonl",
            }
        }


class FetchStreamRequest(BaseModel):
    """Request model for fetching stream file from URL"""
    url: str = Field(..., description="URL to fetch JSON/JSONL file from")
    
    class Config:
        json_schema_extra = {
            "example": {
                "url": "https://example.com/stream.jsonl",
            }
        }


# ============================================================================
# RESPONSE MODELS
# ============================================================================

class SelectorDiscoveryResponse(BaseModel):
    """Response model for selector discovery"""
    job_id: str
    message: str
    status: Literal["running", "completed", "failed"]
    result: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class ScrapingResponse(BaseModel):
    """Response model for article scraping"""
    job_id: str
    message: str
    status: Literal["running", "completed", "failed"]
    result: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class CleaningResponse(BaseModel):
    """Response model for article cleaning"""
    job_id: str
    message: str
    status: Literal["running", "completed", "failed"]
    result: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobStatus(BaseModel):
    """Job status model"""
    job_id: str
    type: Literal["discover", "scrape", "clean"]
    status: Literal["pending", "running", "completed", "failed"]
    progress: int = Field(0, ge=0, le=100, description="Progress percentage")
    created_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class SiteStatus(BaseModel):
    """Site status model"""
    domain: str
    selector_discovery_status: str
    sitemap_status: str
    css_fallback_status: str
    extraction_path: str
    raw_articles_count: int
    cleaned_articles_count: int
    overall_status: str
    last_updated: Optional[datetime] = None


class StatusResponse(BaseModel):
    """Overall status response"""
    total_sites: int
    sites_with_sitemap: int
    sites_with_css_only: int
    sites_failed: int
    total_raw_articles: int
    total_cleaned_articles: int
    sites: List[SiteStatus]
    last_updated: datetime

