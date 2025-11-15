"""
MongoDB storage for pipeline overview data

This module provides the same interface as overview_store.py but uses MongoDB
instead of SQLite for storage.

Environment variables:
    MONGO_HOST: MongoDB host (default: localhost)
    MONGO_PORT: MongoDB port (default: 27017)
    MONGO_DB_NAME: Database name (default: news_scraper)
    MONGO_COLLECTION_NAME: Collection name (default: pipelines_overview)
"""
import os
import csv
import time
from typing import Dict, Any, List, Optional
from datetime import datetime

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, OperationFailure, DuplicateKeyError
except ImportError:
    raise ImportError(
        "pymongo is required. Install it with: pip install pymongo>=4.6.0"
    )

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except Exception:
    pass

# MongoDB connection settings
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_scraper")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "pipelines_overview")

# Default CSV path
BASE_DIR = os.path.dirname(__file__) or "."
DEFAULT_CSV_PATH = os.path.join(BASE_DIR, "pipelines_overview.csv")

# CSV header (and document fields) in exact order - must match overview_store.py
CSV_HEADER: List[str] = [
    "Domain (sources)",
    "Selector Discovery Attempted",
    "Selector Discovery Not Attempted Reason",
    "Selector Discovery Attempt Error",
    "Selector Discovery Attempt Error Response",
    "Sitemap Processing Status",
    "Sitemap Processing Error Details",
    "leaf Sitemap URLs Discovered",
    "CSS Fallback Status",
    "CSS Fallback error Details",
    "Which Path Used for Final Extraction",
    "Total Time (sec) in scraping",
    "Raw Articles scraped",
    "Zero Raw Articles Reason",
    "Cleaning Status",
    "Cleaned Articles (Final)",
    "Duplicates Removed",
    "Missing Dates Removed",
    "Missing Titles Removed",
    "Out of Range/Old Date Removed",
    "Overall pipelines Status",
    "Overall pipelines Error Details",
    "Overall pipelines Explanation",
    "Leaf Sitemap URLs",
    # Diagnostics columns (selection pipeline)
    "initial_url_http_status",
    "robots_browser_retry_attempted",
    "robots_browser_retry_status",
    "robots_txt_found",
    "robots_txt_sitemaps_present",
    "heuristic_fallback_used",
    "heuristic_fallback_status",
    "heuristic_block_detected",
    "heuristic_browser_retry_attempted",
    "heuristic_browser_retry_status",
    "sitemaps_rejected_count",
    "leaf_sitemaps_accepted_count",
    "css_fallback_status",
    "css_fallback_error_details",
    # Filter counts
    "word_after_count",
    "word_rejected_count",
    "year_after_count",
    "year_rejected_count",
    "date_after_count",
    "date_rejected_count",
    # Expansion counts
    "expansion_children_found",
    "children_rejected_word_count",
    "children_rejected_year_count",
    "children_rejected_date_count",
    "leaf_checked_count",
    "leaf_recent_passed_count",
    # Selector detection counts
    "selector_total_leaves",
    "selector_success_count",
    "selector_failed_count",
    "selector_first_error",
    # Leaf extra metrics
    "leaf_total_count",
    "leaf_recency_rejected_count",
    "leaf_title_rejected_count",
]


def _default_row(domain: str) -> Dict[str, str]:
    """Create a default row/document with all fields initialized"""
    return {
        "Domain (sources)": domain,
        "Selector Discovery Attempted": "No",
        "Selector Discovery Not Attempted Reason": "",
        "Selector Discovery Attempt Error": "",
        "Selector Discovery Attempt Error Response": "",
        "Sitemap Processing Status": "Not Attempted",
        "Sitemap Processing Error Details": "",
        "leaf Sitemap URLs Discovered": "0",
        "CSS Fallback Status": "Not Attempted",
        "CSS Fallback error Details": "",
        "Which Path Used for Final Extraction": "Neither",
        "Total Time (sec) in scraping": "0",
        "Raw Articles scraped": "0",
        "Zero Raw Articles Reason": "",
        "Cleaning Status": "Not Attempted",
        "Cleaned Articles (Final)": "0",
        "Duplicates Removed": "0",
        "Missing Dates Removed": "0",
        "Missing Titles Removed": "0",
        "Out of Range/Old Date Removed": "0",
        "Overall pipelines Status": "Pending",
        "Overall pipelines Error Details": "",
        "Overall pipelines Explanation": "",
        "Leaf Sitemap URLs": "",
        # Diagnostics defaults
        "initial_url_http_status": "",
        "robots_browser_retry_attempted": "",
        "robots_browser_retry_status": "",
        "robots_txt_found": "",
        "robots_txt_sitemaps_present": "",
        "heuristic_fallback_used": "",
        "heuristic_fallback_status": "",
        "heuristic_block_detected": "",
        "heuristic_browser_retry_attempted": "",
        "heuristic_browser_retry_status": "",
        "sitemaps_rejected_count": "",
        "leaf_sitemaps_accepted_count": "",
        "css_fallback_status": "",
        "css_fallback_error_details": "",
        # Filter counts defaults
        "word_after_count": "0",
        "word_rejected_count": "0",
        "year_after_count": "0",
        "year_rejected_count": "0",
        "date_after_count": "0",
        "date_rejected_count": "0",
        # Expansion counts defaults
        "expansion_children_found": "0",
        "children_rejected_word_count": "0",
        "children_rejected_year_count": "0",
        "children_rejected_date_count": "0",
        "leaf_checked_count": "0",
        "leaf_recent_passed_count": "0",
        # Selector counts defaults
        "selector_total_leaves": "0",
        "selector_success_count": "0",
        "selector_failed_count": "0",
        "selector_first_error": "",
        # Leaf extra metrics defaults
        "leaf_total_count": "0",
        "leaf_recency_rejected_count": "0",
        "leaf_title_rejected_count": "0",
    }


def _merge_overall_error(prev: str, new_seg: str, max_len: int = 300) -> str:
    """Merge error details with pipe separator"""
    try:
        prev = (prev or "").strip()
        new_seg = (new_seg or "").strip()
        if not new_seg:
            return prev[:max_len]
        parts = [s.strip() for s in prev.split(" | ") if s.strip()] if prev else []
        if new_seg not in parts:
            parts.append(new_seg)
        out = " | ".join(parts)
        return out[:max_len]
    except Exception:
        return (new_seg or prev or "")[:max_len]


def _merge_friendly_explanation(prev: str, new_sentence: str, max_len: int = 300) -> str:
    """Merge friendly explanations with pipe separator"""
    try:
        prev = (prev or "").strip()
        new_sentence = (new_sentence or "").strip()
        if not new_sentence:
            return prev[:max_len]
        parts = [s.strip() for s in prev.split(" | ") if s.strip()] if prev else []
        if new_sentence not in parts:
            parts.append(new_sentence)
        out = " | ".join(parts)
        return out[:max_len]
    except Exception:
        return (new_sentence or prev or "")[:max_len]


def _get_client() -> MongoClient:
    """Get MongoDB client with connection string"""
    connection_string = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
    return MongoClient(
        connection_string,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=5000,
    )


def _get_collection():
    """Get MongoDB collection, creating database/collection if needed"""
    client = _get_client()
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    return collection, client


def init_db() -> None:
    """
    Initialize MongoDB database and collection.
    Creates indexes for efficient queries.
    """
    try:
        collection, client = _get_collection()
        
        # Create index on domain field (primary key equivalent)
        # Use create_index with exist_ok behavior (ignore if already exists)
        try:
            collection.create_index(
                "Domain (sources)",
                unique=True,
                name="domain_unique_idx"
            )
        except Exception:
            # Index might already exist, which is fine
            pass
        
        # Create index on status fields for faster queries
        # These are non-unique, so create_index will be idempotent
        try:
            collection.create_index("Overall pipelines Status")
            collection.create_index("Sitemap Processing Status")
            collection.create_index("CSS Fallback Status")
        except Exception:
            # Indexes might already exist, which is fine
            pass
        
        client.close()
    except ConnectionFailure as e:
        raise ConnectionError(
            f"Failed to connect to MongoDB at {MONGO_HOST}:{MONGO_PORT}. "
            f"Make sure MongoDB is running. Error: {e}"
        )
    except Exception as e:
        # Log but don't fail - collection might already exist
        pass


def upsert_overview(domain: str, updates: Dict[str, Any]) -> None:
    """
    Insert or update a domain's overview data in MongoDB.
    
    Args:
        domain: The domain name (primary key)
        updates: Dictionary of field updates to apply
    """
    if not domain:
        return
    
    # Retry on connection issues
    for attempt in range(5):
        try:
            collection, client = _get_collection()
            
            # Fetch existing document
            existing = collection.find_one({"Domain (sources)": domain})
            
            if existing is None:
                current = _default_row(domain)
            else:
                # Convert existing document to dict, ensuring all fields exist
                current = _default_row(domain)
                for key in CSV_HEADER:
                    if key in existing:
                        current[key] = str(existing[key]) if existing[key] is not None else ""
            
            # Apply updates with merge rules
            current["Domain (sources)"] = domain
            for k, v in (updates or {}).items():
                if k not in CSV_HEADER or v is None:
                    continue
                
                # Apply merge logic for special fields
                if k == "Overall pipelines Error Details":
                    current[k] = _merge_overall_error(current.get(k) or "", str(v))
                elif k == "Overall pipelines Explanation":
                    current[k] = _merge_friendly_explanation(current.get(k) or "", str(v))
                else:
                    current[k] = str(v)
            
            # Ensure all columns exist
            for h in CSV_HEADER:
                current.setdefault(h, "")
            
            # Add timestamp
            current["updated_at"] = datetime.utcnow().isoformat()
            
            # Upsert document
            collection.replace_one(
                {"Domain (sources)": domain},
                current,
                upsert=True
            )
            
            client.close()
            return
            
        except ConnectionFailure as e:
            if attempt < 4:
                time.sleep(0.05 * (2 ** attempt))
                continue
            # Last attempt failed
            return
        except Exception as e:
            # Log error but don't fail silently in development
            if os.getenv("DEBUG", "").lower() in ("1", "true", "yes"):
                print(f"Warning: Error upserting {domain}: {e}")
            return


def export_csv(csv_path: str = DEFAULT_CSV_PATH, db_path: Optional[str] = None) -> None:
    """
    Export all documents from MongoDB to CSV file.
    
    Args:
        csv_path: Path to output CSV file
        db_path: Ignored (kept for compatibility with overview_store interface)
    """
    tmp = csv_path + ".tmp"
    
    try:
        collection, client = _get_collection()
        
        # Fetch all documents, sorted by domain
        cursor = collection.find().sort("Domain (sources)", 1)
        documents = list(cursor)
        
        # Write to temporary CSV file
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
            writer.writeheader()
            
            for doc in documents:
                # Build row from document, ensuring all fields are present
                row = {}
                for header in CSV_HEADER:
                    value = doc.get(header, "")
                    # Convert None to empty string, ensure string type
                    row[header] = str(value) if value is not None else ""
                writer.writerow(row)
        
        client.close()
        
        # Atomic file replacement
        try:
            os.replace(tmp, csv_path)
        except Exception:
            # Fallback for Windows
            try:
                if os.path.exists(csv_path):
                    os.remove(csv_path)
            except Exception:
                pass
            try:
                os.rename(tmp, csv_path)
            except Exception:
                pass
                
    except ConnectionFailure as e:
        # If connection fails, create empty CSV with headers
        try:
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
                writer.writeheader()
        except Exception:
            pass
    except Exception as e:
        # Clean up temp file on error
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

