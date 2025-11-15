"""
MongoDB storage for news articles

Stores cleaned articles in MongoDB with date-based organization and URL deduplication.
"""
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from urllib.parse import urlparse

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, DuplicateKeyError
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
ARTICLES_COLLECTION_NAME = os.getenv("ARTICLES_COLLECTION_NAME", "articles")


def _get_client() -> MongoClient:
    """Get MongoDB client"""
    connection_string = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
    return MongoClient(
        connection_string,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=5000,
    )


def _get_collection():
    """Get MongoDB articles collection"""
    client = _get_client()
    db = client[MONGO_DB_NAME]
    collection = db[ARTICLES_COLLECTION_NAME]
    return collection, client


def init_articles_db() -> None:
    """Initialize MongoDB articles collection with indexes"""
    try:
        collection, client = _get_collection()
        
        # Create unique index on canonical URL to prevent duplicates
        try:
            collection.create_index(
                "canonical_url",
                unique=True,
                name="canonical_url_unique_idx"
            )
        except Exception:
            pass
        
        # Create indexes for efficient queries
        try:
            collection.create_index("date")
            collection.create_index("date_published")
            collection.create_index("source")
            collection.create_index([("date", -1)])  # Descending for recent first
            collection.create_index([("created_at", -1)])
        except Exception:
            pass
        
        client.close()
    except ConnectionFailure as e:
        raise ConnectionError(
            f"Failed to connect to MongoDB at {MONGO_HOST}:{MONGO_PORT}. "
            f"Make sure MongoDB is running. Error: {e}"
        )
    except Exception:
        pass


def _canonicalize_url(url: str) -> str:
    """Normalize URL for deduplication"""
    try:
        parsed = urlparse(url)
        # Lowercase domain, remove fragment, normalize path
        canonical = f"{parsed.scheme}://{parsed.netloc.lower()}{parsed.path}"
        if parsed.query:
            # Remove common tracking parameters
            from urllib.parse import parse_qs, urlencode
            params = parse_qs(parsed.query, keep_blank_values=True)
            filtered = {k: v for k, v in params.items() 
                       if not k.lower().startswith(('utm_', 'fbclid', 'gclid'))}
            if filtered:
                canonical += "?" + urlencode(filtered, doseq=True)
        return canonical.rstrip('/')
    except Exception:
        return url.lower().strip()


def save_article(article: Dict[str, Any]) -> bool:
    """
    Save article to MongoDB with deduplication.
    
    Args:
        article: Article dict with keys: title, url, summary, date, source
        
    Returns:
        True if saved (new), False if duplicate
    """
    if not article or not article.get("url"):
        return False
    
    try:
        collection, client = _get_collection()
        
        # Canonicalize URL for deduplication
        canonical_url = _canonicalize_url(article["url"])
        
        # Parse date
        date_published = None
        if article.get("date"):
            try:
                if isinstance(article["date"], str):
                    date_published = datetime.fromisoformat(article["date"].replace("Z", "+00:00"))
                elif isinstance(article["date"], datetime):
                    date_published = article["date"]
            except Exception:
                pass
        
        # Prepare document
        doc = {
            "title": article.get("title", "").strip(),
            "url": article["url"],
            "canonical_url": canonical_url,
            "summary": article.get("summary", "").strip(),
            "source": article.get("source", "").strip(),
            "date": date_published or datetime.now(timezone.utc),
            "date_published": date_published,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        
        # Try to insert (will fail if duplicate)
        try:
            collection.insert_one(doc)
            client.close()
            return True
        except DuplicateKeyError:
            # Article already exists, update timestamp
            collection.update_one(
                {"canonical_url": canonical_url},
                {"$set": {"updated_at": datetime.now(timezone.utc)}}
            )
            client.close()
            return False
            
    except Exception:
        try:
            client.close()
        except Exception:
            pass
        return False


def save_articles_batch(articles: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Save multiple articles with deduplication.
    
    Returns:
        Dict with counts: saved, duplicates
    """
    saved = 0
    duplicates = 0
    
    for article in articles:
        if save_article(article):
            saved += 1
        else:
            duplicates += 1
    
    return {"saved": saved, "duplicates": duplicates}


def get_articles(
    limit: int = 100,
    skip: int = 0,
    source: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Get articles from MongoDB"""
    try:
        collection, client = _get_collection()
        
        # Build query
        query = {}
        if source:
            query["source"] = source
        if date_from or date_to:
            date_query = {}
            if date_from:
                date_query["$gte"] = date_from
            if date_to:
                date_query["$lte"] = date_to
            if date_query:
                query["date"] = date_query
        
        # Fetch articles
        cursor = collection.find(query).sort("date", -1).skip(skip).limit(limit)
        articles = list(cursor)
        
        # Convert ObjectId to string and format dates
        for article in articles:
            article["_id"] = str(article["_id"])
            if article.get("date"):
                article["date"] = article["date"].isoformat()
            if article.get("date_published"):
                article["date_published"] = article["date_published"].isoformat()
            if article.get("created_at"):
                article["created_at"] = article["created_at"].isoformat()
            if article.get("updated_at"):
                article["updated_at"] = article["updated_at"].isoformat()
        
        client.close()
        return articles
    except Exception as e:
        try:
            client.close()
        except Exception:
            pass
        return []


def get_articles_count(
    source: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> int:
    """Get total count of articles"""
    try:
        collection, client = _get_collection()
        
        query = {}
        if source:
            query["source"] = source
        if date_from or date_to:
            date_query = {}
            if date_from:
                date_query["$gte"] = date_from
            if date_to:
                date_query["$lte"] = date_to
            if date_query:
                query["date"] = date_query
        
        count = collection.count_documents(query)
        client.close()
        return count
    except Exception:
        try:
            client.close()
        except Exception:
            pass
        return 0

