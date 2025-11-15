"""
Service layer wrapping pipeline functions
"""
import os
import json
import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import concurrent.futures as cf

# Import pipeline modules
import selection_extraction_pipeline as sep
import stream_scraping_pipeline as ssp
import clean_selection_entries as cse
# Use MongoDB instead of SQLite
try:
    from mongodb_store import init_db, export_csv, upsert_overview
except ImportError:
    # Fallback to SQLite if MongoDB not available
    from overview_store import init_db, export_csv, upsert_overview


class SelectionService:
    """Service for selector discovery"""
    
    async def discover_selectors(
        self,
        urls: List[str],
        recent_hours: int = 24,
        site_concurrency: int = 1,
        llm_concurrency: int = 3,
        timeout: float = 15.0,
        max_depth: int = 2,
    ) -> Dict[str, Any]:
        """
        Discover selectors for given URLs.
        
        This wraps the selection_extraction_pipeline functionality.
        """
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        
        def _run_discovery():
            # Initialize DB
            try:
                init_db()
            except Exception:
                pass
            
            # Stream file path
            stream_file = "selection_extraction_report_stream.jsonl"
            
            # Process each URL
            results = []
            
            def _process_url(url: str) -> Dict[str, Any]:
                try:
                    # Call the main processing function from selection_extraction_pipeline
                    stats = sep.test_recursive_expansion(
                        url=url,
                        recent_hours=recent_hours,
                        timeout=timeout,
                        max_depth=max_depth,
                        llm_concurrency=llm_concurrency,
                    )
                    
                    # Stream result
                    from datetime import datetime
                    sep._append_stream({
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "result": stats
                    }, stream_path=stream_file)
                    
                    # Update MongoDB
                    try:
                        from urllib.parse import urlparse
                        
                        domain = urlparse(url).netloc or url
                        robots = stats.get("robotsTxt") or {}
                        final_stats = stats.get("finalStats") or {}
                        cssf = stats.get("cssFallback") or {}
                        llm = stats.get("llmDetection") or {}
                        
                        leaves = int(final_stats.get("afterDateFilter") or 0)
                        
                        # Build updates
                        updates = {
                            "Selector Discovery Attempted": "Yes",
                            "Sitemap Processing Status": "Success" if leaves > 0 else "Empty",
                            "leaf Sitemap URLs Discovered": str(leaves),
                            "CSS Fallback Status": "Success" if cssf.get("success") else ("Not Attempted" if not cssf.get("triggered") else "Error"),
                        }
                        
                        upsert_overview(domain, updates)
                    except Exception:
                        pass
                    
                    return {
                        "url": url,
                        "status": "completed",
                        "stats": stats,
                    }
                except Exception as e:
                    # Stream error result
                    try:
                        from datetime import datetime
                        sep._append_stream({
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "result": sep._default_stats(url=url, err=str(e))
                        }, stream_path=stream_file)
                    except Exception:
                        pass
                    
                    return {
                        "url": url,
                        "status": "failed",
                        "error": str(e),
                    }
            
            # Process URLs with concurrency
            if site_concurrency == 1 or len(urls) == 1:
                for url in urls:
                    result = _process_url(url)
                    results.append(result)
            else:
                with cf.ThreadPoolExecutor(max_workers=site_concurrency) as executor:
                    futures = [executor.submit(_process_url, url) for url in urls]
                    for future in cf.as_completed(futures):
                        try:
                            result = future.result()
                            results.append(result)
                        except Exception as e:
                            results.append({
                                "url": "unknown",
                                "status": "failed",
                                "error": str(e),
                            })
            
            # Export CSV
            try:
                export_csv()
            except Exception:
                pass
            
            return {
                "total_urls": len(urls),
                "completed": len([r for r in results if r.get("status") == "completed"]),
                "failed": len([r for r in results if r.get("status") == "failed"]),
                "results": results,
                "stream_file": stream_file,
            }
        
        return await loop.run_in_executor(None, _run_discovery)


class ScrapingService:
    """Service for article scraping"""
    
    async def scrape_articles(
        self,
        stream_path: Optional[str] = None,
        mode: str = "auto",
        site_concurrency: int = 1,
        target_concurrency: int = 6,
        timeout: float = 15.0,
        max_items: int = 500,
    ) -> Dict[str, Any]:
        """
        Scrape articles using discovered selectors.
        
        This wraps the stream_scraping_pipeline functionality.
        """
        loop = asyncio.get_event_loop()
        
        # Default stream path
        if not stream_path:
            stream_path = "selection_extraction_report_stream.jsonl"
        
        def _run_scraping():
            # Initialize DB
            try:
                init_db()
            except Exception:
                pass
            
            # Check if stream file exists
            if not os.path.exists(stream_path):
                return {
                    "status": "failed",
                    "error": f"Stream file not found: {stream_path}",
                }
            
            # Set timeout globally for the scraping pipeline
            ssp.SCRAPE_TIMEOUT = timeout
            
            # Output path
            output_path = "stream_scraped_articles.jsonl"
            
            # Use the stream scraping pipeline's internal functions
            # We'll process sites from the stream
            from stream_scraping_pipeline import (
                _read_jsonl_once,
                _normalize_targets,
                _scrape_sitemap_target,
                _scrape_css_target,
                Writer,
                StatsCollector,
            )
            from urllib.parse import urlparse
            
            processed_sites = set()
            site_rows = _read_jsonl_once(stream_path)
            
            # Initialize writer and stats
            writer = Writer(output_path, queue_size=100)
            writer.start()
            
            log_sites_path = "stream_scrape_sites_log.jsonl"
            log_summary_path = "stream_scrape_summary.json"
            collector = StatsCollector(log_sites_path, log_summary_path, append=False)
            collector.start_global()
            
            def _process_site(row: Dict[str, Any]) -> tuple:
                from datetime import datetime, timezone
                import time
                
                site = ((row.get('result') or {}).get('url') or '').strip()
                if not site or site in processed_sites:
                    return site, 0
                
                processed_sites.add(site)
                targets = _normalize_targets(row)
                
                if not targets:
                    return site, 0
                
                total_items = 0
                started_iso = datetime.now(timezone.utc).isoformat()
                start_perf = time.perf_counter()
                items_by_source = {"sitemap": 0, "css": 0}
                approaches_used = []
                
                # Process targets with concurrency
                with cf.ThreadPoolExecutor(max_workers=target_concurrency) as ex:
                    futures = []
                    fut_type = {}
                    
                    for t in targets:
                        t_type = t.get('type')
                        use_mode = mode
                        if use_mode == 'auto':
                            use_mode = t_type
                        if use_mode not in ('sitemap', 'css', 'both'):
                            use_mode = t_type
                        
                        if use_mode in ('sitemap', 'both') and t_type == 'sitemap':
                            f = ex.submit(_scrape_sitemap_target, t)
                            futures.append(f)
                            fut_type[f] = 'sitemap'
                        
                        if use_mode in ('css', 'both') and t_type == 'css':
                            f = ex.submit(_scrape_css_target, t, headful=False, slowmo_ms=0, max_items=max_items)
                            futures.append(f)
                            fut_type[f] = 'css'
                    
                    approaches_used = sorted(list(set(fut_type.values())))
                    
                    for fut in cf.as_completed(futures):
                        try:
                            items = fut.result() or []
                        except Exception:
                            items = []
                        
                        src = fut_type.get(fut, 'unknown')
                        total_items += len(items)
                        if src in items_by_source:
                            items_by_source[src] += len(items)
                        
                        for it in items:
                            writer.submit({
                                'site': site,
                                'sourceType': src,
                                'item': it,
                                'ts': time.strftime('%Y-%m-%d %H:%M:%S')
                            })
                
                end_perf = time.perf_counter()
                ended_iso = datetime.now(timezone.utc).isoformat()
                collector.record_site(
                    site=site,
                    started_at_iso=started_iso,
                    ended_at_iso=ended_iso,
                    duration_sec=(end_perf - start_perf),
                    items_by_source=items_by_source,
                    approaches_used=approaches_used
                )
                
                return site, total_items
            
            # Process sites with concurrency
            completed = 0
            total_articles = 0
            with cf.ThreadPoolExecutor(max_workers=site_concurrency) as site_pool:
                futures = [site_pool.submit(_process_site, row) for row in site_rows]
                for fut in cf.as_completed(futures):
                    try:
                        site, count = fut.result()
                        total_articles += count
                        completed += 1
                    except Exception:
                        completed += 1
            
            # Finalize
            writer.close()
            collector.end_global()
            collector.write_summary()
            
            # Export CSV
            try:
                export_csv()
            except Exception:
                pass
            
            return {
                "status": "completed",
                "stream_path": stream_path,
                "output_path": output_path,
                "sites_processed": completed,
                "total_articles": total_articles,
                "mode": mode,
            }
        
        return await loop.run_in_executor(None, _run_scraping)


class CleaningService:
    """Service for article cleaning"""
    
    async def clean_articles(
        self,
        input_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Clean and filter scraped articles.
        
        This wraps the clean_selection_entries functionality.
        """
        loop = asyncio.get_event_loop()
        
        # Default input path
        if not input_path:
            input_path = "stream_scraped_articles.jsonl"
        
        def _run_cleaning():
            # Initialize DB
            try:
                init_db()
            except Exception:
                pass
            
            # Run cleaning
            try:
                summary = cse.clean_offline_from_streamed_articles(
                    input_path=input_path,
                )
                
                # Export CSV
                try:
                    export_csv()
                except Exception:
                    pass
                
                return {
                    "status": "completed",
                    "summary": summary,
                }
            except Exception as e:
                return {
                    "status": "failed",
                    "error": str(e),
                }
        
        return await loop.run_in_executor(None, _run_cleaning)


class StatusService:
    """Service for status and monitoring"""
    
    async def get_status(
        self,
        domain: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Get overall pipeline status"""
        loop = asyncio.get_event_loop()
        
        def _get_status():
            try:
                init_db()
            except Exception:
                pass
            
            # Read from CSV or SQLite
            csv_path = "pipelines_overview.csv"
            sites = []
            
            if os.path.exists(csv_path):
                import csv as csv_module
                with open(csv_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv_module.DictReader(f)
                    for row in reader:
                        if domain:
                            site_domain = row.get("Domain (sources)", "")
                            if domain.lower() not in site_domain.lower():
                                continue
                        sites.append(row)
            
            # Calculate statistics
            total_sites = len(sites)
            sites_with_sitemap = sum(
                1 for s in sites
                if s.get("Sitemap Processing Status", "").lower() in ("success", "completed")
            )
            sites_with_css_only = sum(
                1 for s in sites
                if s.get("CSS Fallback Status", "").lower() in ("success", "completed")
                and s.get("Sitemap Processing Status", "").lower() not in ("success", "completed")
            )
            sites_failed = sum(
                1 for s in sites
                if s.get("Overall pipelines Status", "").lower() == "error"
            )
            
            total_raw = sum(
                int(s.get("Raw Articles scraped", "0") or "0")
                for s in sites
            )
            total_cleaned = sum(
                int(s.get("Cleaned Articles (Final)", "0") or "0")
                for s in sites
            )
            
            # Convert CSV rows to SiteStatus-like dicts
            sites_list = []
            for site_row in sites[:limit]:
                sites_list.append({
                    "domain": site_row.get("Domain (sources)", ""),
                    "selector_discovery_status": site_row.get("Selector Discovery Attempted", "No"),
                    "sitemap_status": site_row.get("Sitemap Processing Status", "Not Attempted"),
                    "css_fallback_status": site_row.get("CSS Fallback Status", "Not Attempted"),
                    "extraction_path": site_row.get("Which Path Used for Final Extraction", "Neither"),
                    "raw_articles_count": int(site_row.get("Raw Articles scraped", "0") or "0"),
                    "cleaned_articles_count": int(site_row.get("Cleaned Articles (Final)", "0") or "0"),
                    "overall_status": site_row.get("Overall pipelines Status", "Pending"),
                })
            
            return {
                "total_sites": total_sites,
                "sites_with_sitemap": sites_with_sitemap,
                "sites_with_css_only": sites_with_css_only,
                "sites_failed": sites_failed,
                "total_raw_articles": total_raw,
                "total_cleaned_articles": total_cleaned,
                "sites": sites_list,
                "last_updated": datetime.utcnow(),
            }
        
        return await loop.run_in_executor(None, _get_status)
    
    async def get_sites_status(
        self,
        domain: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get status of all sites"""
        status = await self.get_status(domain=domain, limit=limit)
        
        sites = []
        for site in status["sites"]:
            sites.append({
                "domain": site.get("Domain (sources)", ""),
                "selector_discovery_status": site.get("Selector Discovery Attempted", "No"),
                "sitemap_status": site.get("Sitemap Processing Status", "Not Attempted"),
                "css_fallback_status": site.get("CSS Fallback Status", "Not Attempted"),
                "extraction_path": site.get("Which Path Used for Final Extraction", "Neither"),
                "raw_articles_count": int(site.get("Raw Articles scraped", "0") or "0"),
                "cleaned_articles_count": int(site.get("Cleaned Articles (Final)", "0") or "0"),
                "overall_status": site.get("Overall pipelines Status", "Pending"),
            })
        
        return sites

