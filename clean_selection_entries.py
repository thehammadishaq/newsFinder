"""
Cleaning pipeline

Modes:
1) OFFLINE (default) - Clean existing stream output without any network:
   - Read python_tools/stream_scraped_articles.jsonl (no internet)
   - Parse dates (timezone-aware when present)
   - Keep only articles published today in article's own timezone
   - Deduplicate strictly by canonical URL
   - Enrich with title (or slug), summary (from local fields), source

2) SELECTOR-ASSISTED (optional) - Use selection_extraction_report_stream.jsonl to expand sitemaps
   (may involve network if used). Not used by default to respect offline requirement.

Outputs:
- python_tools/articles_clean_current.jsonl
- python_tools/articles_removed_no_date.jsonl
- python_tools/articles_removed_out_of_current_day.jsonl
- python_tools/articles_removed_duplicate.jsonl
- python_tools/articles_cleaning_summary.json
"""

import os
import json
from overview_store import init_db as ov_init_db, upsert_overview as ov_upsert, export_csv as ov_export_csv
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from datetime import datetime, timezone
import time
import csv

try:
    from dateutil import parser as du_parser
except Exception:
    du_parser = None  # We'll fall back to strict ISO parsing if unavailable

# Reuse sitemap utilities
from sitemap_discovery import (
    expand_sitemap_entries_all,
    expand_sitemap_entries_recent,
)
import concurrent.futures as cf


def _iter_jsonl(path: str) -> Iterable[dict]:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = (line or "").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue


def _collect_sitemap_fields(stream_path: str) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    """Collect unique leaf sitemap URLs and any detected field selectors per sitemap.

    Returns (sitemaps, fields_map) where fields_map[sitemap_url] = { field_name -> xpath }
    """
    sitemaps: List[str] = []
    seen = set()
    fields_map: Dict[str, Dict[str, str]] = {}

    for rec in _iter_jsonl(stream_path):
        result = rec.get("result") if isinstance(rec, dict) else None
        if not isinstance(result, dict):
            continue
        llm = result.get("llmDetection") or {}
        selectors = llm.get("selectors")
        if not isinstance(selectors, list):
            continue
        for sel in selectors:
            if not isinstance(sel, dict):
                continue
            sm_url = sel.get("url")
            det = sel.get("detectedSelectors") or {}
            fields = det.get("fields") if isinstance(det, dict) else None
            if not sm_url or not isinstance(sm_url, str):
                continue
            if sm_url not in seen:
                seen.add(sm_url)
                sitemaps.append(sm_url)
            if isinstance(fields, dict) and fields:
                fields_map[sm_url] = fields

    return sitemaps, fields_map


def _canonicalize_url(raw_url: str) -> str:
    """Normalize URL for deduplication: lower host, strip tracking params, drop fragment."""
    try:
        parts = urlsplit(raw_url)
        scheme = parts.scheme or "https"
        netloc = (parts.netloc or "").lower()
        path = parts.path or "/"
        # Filter query params
        bad_prefixes = ("utm_",)
        bad_keys = {"gclid", "fbclid"}
        query_pairs = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            lk = (k or "").lower()
            if lk in bad_keys:
                continue
            if any(lk.startswith(p) for p in bad_prefixes):
                continue
            query_pairs.append((k, v))
        new_query = urlencode(query_pairs, doseq=True)
        # Remove trailing slash normalization only if not root
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return urlunsplit((scheme, netloc, path, new_query, ""))
    except Exception:
        return raw_url


def _parse_date_any(date_str: Optional[str]) -> Optional[datetime]:
    """Parse many date formats. Return timezone-aware datetime if possible.

    Priority:
    - dateutil parser (handles RFC/ISO/locale formats, offsets)
    - strict ISO-8601 subset via fromisoformat
    """
    if not date_str:
        return None
    s = (date_str or "").strip()
    if not s:
        return None
    # Fast path: strict ISO-8601
    try:
        iso = s
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    # Fallback: dateutil parse (robust, heavier)
    if du_parser is not None:
        try:
            dt = du_parser.parse(s)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
        try:
            dt = du_parser.parse(s, dayfirst=True)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    return None


def _is_same_local_day(article_dt: datetime) -> bool:
    """Compare article_dt to 'today' in the article's own timezone."""
    try:
        tz = article_dt.tzinfo or timezone.utc
        today_local = datetime.now(tz).date()
        return article_dt.astimezone(tz).date() == today_local
    except Exception:
        return False


def _domain_from_url(url: str) -> str:
    try:
        return (urlsplit(url).netloc or "").lower()
    except Exception:
        return ""


def _slug_title_from_url(url: str) -> str:
    try:
        path = urlsplit(url).path or "/"
        seg = path.rstrip("/").split("/")[-1]
        # Remove extension and common id-like tails
        base = seg.split(".")[0]
        # Split by dashes/underscores
        words = [w for w in re_split(r"[-_]+", base) if w]
        title = " ".join(words).strip()
        return title.title() if title else base.title()
    except Exception:
        return url


def re_split(pattern: str, text: str) -> List[str]:
    import re as _re
    return _re.split(pattern, text)


def _build_summary_from_fields(e: dict) -> Optional[str]:
    desc = e.get("description") or e.get("summary") or e.get("image_caption") or ""
    parts: List[str] = []
    if desc:
        parts.append(desc.strip())
    def _add(label: str, key: str) -> None:
        val = e.get(key)
        if isinstance(val, str) and val.strip():
            parts.append(f"{label}: {val.strip()}")
    # Common extras
    _add("Keywords", "keywords")
    _add("Tickers", "stock_tickers")
    _add("Publication", "publication_name")
    _add("Image", "image_url")
    _add("ImgCap", "image_caption")
    return " | ".join(parts) if parts else None


def _source_name_from_domain(domain: str) -> str:
    d = (domain or "").lower()
    if d.endswith("apnews.com"):
        return "AP News"
    if d.endswith("reuters.com"):
        return "Reuters"
    # Fallback: strip www and title-case registrable-ish label
    host = d.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    label = host.split(".")[0]
    return label.title() if label else d


# ========================
# CSV upsert helper (shared contract)
# ========================
_CSV_PATH = os.path.join(os.path.dirname(__file__) or ".", "pipelines_overview.csv")
_CSV_HEADER = [
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
]


def _read_csv_map(path: str) -> Dict[str, Dict[str, str]]:
    rows: Dict[str, Dict[str, str]] = {}
    if not os.path.exists(path):
        return rows
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                d = (row.get(_CSV_HEADER[0]) or "").strip()
                if d:
                    rows[d] = {k: (row.get(k) or "") for k in _CSV_HEADER}
    except Exception:
        pass
    return rows


def _default_row(domain: str) -> Dict[str, str]:
    return {
        "Domain (sources)": domain,
        "Selector Discovery Attempted": "No",
        "Selector Discovery Not Attempted Reason": "",
        "Selector Discovery Attempt Error": "",
        "Sitemap Processing Status": "Not Attempted",
        "Sitemap Processing Error Details": "",
        "leaf Sitemap URLs Discovered": "0",
        "CSS Fallback Status": "Not Attempted",
        "CSS Fallback error Details": "",
        "Which Path Used for Final Extraction": "Neither",
        "Total Time (sec) in scraping": "0",
        "Raw Articles scraped": "0",
        "Cleaning Status": "Not Attempted",
        "Cleaned Articles (Final)": "0",
        "Duplicates Removed": "0",
        "Missing Dates Removed": "0",
        "Missing Titles Removed": "0",
        "Out of Range/Old Date Removed": "0",
        "Overall pipelines Status": "Pending",
        "Leaf Sitemap URLs": "",
    }


def _write_csv_map(path: str, rows: Dict[str, Dict[str, str]]) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=_CSV_HEADER)
            w.writeheader()
            for domain in sorted(rows.keys()):
                row = rows[domain]
                for h in _CSV_HEADER:
                    row.setdefault(h, "")
                w.writerow(row)
        try:
            os.replace(tmp, path)
        except Exception:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass
            try:
                os.rename(tmp, path)
            except Exception:
                pass
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _upsert_csv_row(domain: str, updates: Dict[str, str]) -> None:
    for _ in range(3):
        try:
            rows = _read_csv_map(_CSV_PATH)
            row = rows.get(domain) or _default_row(domain)
            row[_CSV_HEADER[0]] = domain
            for k, v in (updates or {}).items():
                if k in _CSV_HEADER and v is not None:
                    if k == "Overall pipelines Error Details":
                        prev = row.get(k) or ""
                        try:
                            parts = [s.strip() for s in (prev or "").split(" | ") if s.strip()]
                            if str(v).strip() and str(v).strip() not in parts:
                                parts.append(str(v).strip())
                            row[k] = " | ".join(parts)[:300]
                        except Exception:
                            row[k] = str(v)[:300]
                    elif k == "Overall pipelines Explanation":
                        prev = row.get(k) or ""
                        try:
                            parts = [s.strip() for s in (prev or "").split(" | ") if s.strip()]
                            sent = str(v).strip()
                            if sent and sent not in parts:
                                parts.append(sent)
                            row[k] = " | ".join(parts)[:300]
                        except Exception:
                            row[k] = str(v)[:300]
                    else:
                        row[k] = str(v)
            rows[domain] = row
            _write_csv_map(_CSV_PATH, rows)
            return
        except PermissionError:
            time.sleep(0.2)
        except Exception:
            return


# ============================
# Optional selector-assisted mode
# ============================
def _expand_entries_for_sitemap(sitemap_url: str, fields_map: Dict[str, Dict[str, str]], timeout: float) -> List[dict]:
    fields = fields_map.get(sitemap_url)
    try:
        if isinstance(fields, dict) and fields:
            entries = expand_sitemap_entries_recent(
                sitemap_url,
                recent_hours=None,
                timeout=timeout,
                max_urls=25000,
                field_selectors=fields,
            )
        else:
            entries = expand_sitemap_entries_all(sitemap_url, timeout=timeout, max_urls=25000)
    except Exception:
        entries = []
    # Attach source sitemap for provenance
    for e in entries:
        if isinstance(e, dict):
            e.setdefault("sourceSitemap", sitemap_url)
    return entries


# ==========================================
# OFFLINE CLEANER: stream_scraped_articles
# ==========================================
def _iter_jsonl_once(path: str) -> Iterable[dict]:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = (line or "").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _extract_from_item(item: dict) -> Tuple[str, str, Optional[str], dict]:
    # url candidates
    url = (
        item.get("url") or item.get("link") or item.get("loc") or ""
    )
    # date candidates (strings)
    date_str = (
        item.get("date")
        or item.get("publication_date")
        or item.get("pubDate")
        or item.get("lastmod")
        or item.get("datetime")
        or item.get("time")
        or ""
    )
    # title direct (optional)
    title = item.get("title")
    # extras used for summary
    extras = {
        "description": item.get("description") or item.get("summary"),
        "keywords": item.get("keywords"),
        "stock_tickers": item.get("stock_tickers"),
        "image_caption": item.get("image_caption"),
        "publication_name": item.get("publication_name"),
        "image_url": item.get("image_url"),
    }
    return str(url), str(date_str), (str(title) if isinstance(title, str) else None), extras


def clean_offline_from_streamed_articles(
    input_path: str = "stream_scraped_articles.jsonl",
    out_clean_path: str = "articles_clean_current.jsonl",
    out_removed_no_date_path: str = "articles_removed_no_date.jsonl",
    out_removed_out_of_current_day_path: str = "articles_removed_out_of_current_day.jsonl",
    out_removed_duplicate_path: str = "articles_removed_duplicate.jsonl",
    out_removed_no_title_path: str = "articles_removed_no_title.jsonl",
    out_summary_path: str = "articles_cleaning_summary.json",
) -> Dict[str, int]:
    total_input = 0
    kept_count = 0
    removed_no_date = 0
    removed_not_today = 0
    removed_duplicate = 0
    removed_no_title = 0

    # Per-site aggregation for CSV (site_domain -> counters)
    per_site: Dict[str, Dict[str, int]] = {}

    # Prepare outputs (truncate)
    try:
        if os.path.exists(out_clean_path):
            os.remove(out_clean_path)
        if os.path.exists(out_removed_no_date_path):
            os.remove(out_removed_no_date_path)
        if os.path.exists(out_removed_out_of_current_day_path):
            os.remove(out_removed_out_of_current_day_path)
        if os.path.exists(out_removed_duplicate_path):
            os.remove(out_removed_duplicate_path)
        if os.path.exists(out_removed_no_title_path):
            os.remove(out_removed_no_title_path)
    except Exception:
        pass

    seen_urls = set()

    start_perf = time.perf_counter()
    with open(out_clean_path, "a", encoding="utf-8") as f_keep, \
         open(out_removed_no_date_path, "a", encoding="utf-8") as f_no_date, \
         open(out_removed_out_of_current_day_path, "a", encoding="utf-8") as f_out_day, \
         open(out_removed_duplicate_path, "a", encoding="utf-8") as f_dup, \
         open(out_removed_no_title_path, "a", encoding="utf-8") as f_no_title:
        for rec in _iter_jsonl_once(input_path):
            # Use original site for grouping
            site_url = rec.get("site") if isinstance(rec, dict) else None
            site_domain = _domain_from_url(site_url) if isinstance(site_url, str) else ""

            item = rec.get("item") if isinstance(rec.get("item"), dict) else rec
            if not isinstance(item, dict):
                continue
            total_input += 1

            raw_url, raw_date, direct_title, extras = _extract_from_item(item)
            if not raw_url:
                # Skip records without URL
                continue

            canonical_url = _canonicalize_url(raw_url)
            domain = _domain_from_url(canonical_url)
            if site_domain and site_domain not in per_site:
                per_site[site_domain] = {
                    "kept": 0,
                    "dup": 0,
                    "no_date": 0,
                    "out_day": 0,
                    "no_title": 0,
                }

            # Deduplicate first
            if canonical_url in seen_urls:
                removed_duplicate += 1
                f_dup.write(json.dumps({
                    "url": raw_url,
                    "canonicalUrl": canonical_url,
                    "domain": domain,
                    "dateOriginal": raw_date,
                    "reason": "duplicate_url",
                }, ensure_ascii=False) + "\n")
                if site_domain:
                    per_site[site_domain]["dup"] += 1
                continue

            # Date presence check
            if not (isinstance(raw_date, str) and raw_date.strip()):
                removed_no_date += 1
                f_no_date.write(json.dumps({
                    "url": raw_url,
                    "canonicalUrl": canonical_url,
                    "domain": domain,
                    "dateOriginal": raw_date,
                    "reason": "no_date",
                }, ensure_ascii=False) + "\n")
                if site_domain:
                    per_site[site_domain]["no_date"] += 1
                continue

            # Parse date
            dt = _parse_date_any(raw_date)
            if dt is None:
                removed_no_date += 1
                f_no_date.write(json.dumps({
                    "url": raw_url,
                    "canonicalUrl": canonical_url,
                    "domain": domain,
                    "dateOriginal": raw_date,
                    "reason": "no_date",
                }, ensure_ascii=False) + "\n")
                if site_domain:
                    per_site[site_domain]["no_date"] += 1
                continue

            # Same local day filter
            if not _is_same_local_day(dt):
                removed_not_today += 1
                f_out_day.write(json.dumps({
                    "url": raw_url,
                    "canonicalUrl": canonical_url,
                    "domain": domain,
                    "dateOriginal": raw_date,
                    "dateParsedISO": dt.astimezone(timezone.utc).isoformat(),
                    "reason": "out_of_current_day",
                }, ensure_ascii=False) + "\n")
                if site_domain:
                    per_site[site_domain]["out_day"] += 1
                continue

            # Require explicit title; filter if missing
            if not (isinstance(direct_title, str) and direct_title.strip()):
                removed_no_title += 1
                f_no_title.write(json.dumps({
                    "url": raw_url,
                    "canonicalUrl": canonical_url,
                    "domain": domain,
                    "dateOriginal": raw_date,
                    "reason": "no_title",
                }, ensure_ascii=False) + "\n")
                if site_domain:
                    per_site[site_domain]["no_title"] += 1
                continue

            # Keep and enrich
            seen_urls.add(canonical_url)
            kept_count += 1

            title = direct_title
            # Build summary from extras
            summary = None
            if isinstance(extras, dict):
                summary = _build_summary_from_fields(extras)
            summary = summary or ""
            source = _source_name_from_domain(domain)

            f_keep.write(json.dumps({
                "title": title,
                "url": raw_url,
                "summary": summary,
                "date": dt.astimezone(timezone.utc).isoformat(),
                "source": source,
            }, ensure_ascii=False) + "\n")
            if site_domain:
                per_site[site_domain]["kept"] += 1

    end_perf = time.perf_counter()
    summary = {
        "totalEntries": total_input,
        "kept": kept_count,
        "removed_no_date": removed_no_date,
        "removed_out_of_current_day": removed_not_today,
        "removed_duplicate_url": removed_duplicate,
        "removed_no_title": removed_no_title,
        "totalDurationSec": round(float(end_perf - start_perf), 3),
        "uniqueLeafSitemaps": 0,
        "runAtUtc": datetime.utcnow().isoformat() + "Z",
        "mode": "offline_stream_clean",
    }

    try:
        with open(out_summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # === CSV upsert per-site: cleaning + overall (SQLite-backed) ===
    try:
        # Ensure DB exists
        try:
            ov_init_db()
        except Exception:
            pass

        # Load current CSV to reference discovery and extraction statuses (best-effort)
        rows = _read_csv_map(_CSV_PATH)
        # Build domain -> existing CSV key map (prefer longest key per host)
        domain_to_key: Dict[str, str] = {}
        try:
            for k in rows.keys():
                host = _domain_from_url(k)
                h = (host or "").lower()
                if h.startswith("www."):
                    h = h[4:]
                if not h:
                    continue
                prev = domain_to_key.get(h)
                if (prev is None) or (len(k) > len(prev)):
                    domain_to_key[h] = k
        except Exception:
            domain_to_key = {}
        for domain, cnt in per_site.items():
            # Cleaning status is Success if we reached here
            updates: Dict[str, str] = {
                "Domain (sources)": domain,
                "Cleaning Status": "Success",
                "Cleaned Articles (Final)": str(int(cnt.get("kept", 0))),
                "Duplicates Removed": str(int(cnt.get("dup", 0))),
                "Missing Dates Removed": str(int(cnt.get("no_date", 0))),
                "Missing Titles Removed": str(int(cnt.get("no_title", 0))),
                "Out of Range/Old Date Removed": str(int(cnt.get("out_day", 0))),
            }

            # Build cleaning error segment (only if nothing kept)
            try:
                kept = int(cnt.get("kept", 0))
                dup = int(cnt.get("dup", 0))
                no_date = int(cnt.get("no_date", 0))
                out_day = int(cnt.get("out_day", 0))
                total = kept + dup + no_date + out_day
                if total > 0 and kept == 0:
                    if dup == total:
                        creason = "all_duplicates"
                    elif no_date == total:
                        creason = "all_no_date"
                    elif out_day == total:
                        creason = "all_out_of_current_day"
                    else:
                        creason = "kept_zero"
                    cctx = f"kept={kept} dup={dup} no_date={no_date} out_day={out_day}"
                    updates["Overall pipelines Error Details"] = f"cleaning: {creason}; {cctx}"
            except Exception:
                pass

            # Compute overall status (map domain -> existing URL-key row; skip if none)
            host = (domain or "").lower()
            if host.startswith("www."):
                host_base = host[4:]
            else:
                host_base = host
            key = domain_to_key.get(host_base) or domain_to_key.get("www." + host_base) or domain
            row = rows.get(key) or _default_row(key)
            discovery_status = (row.get("Sitemap Processing Status") or "").strip()
            which_path = (row.get("Which Path Used for Final Extraction") or "").strip()
            cleaning_status = updates.get("Cleaning Status")

            overall = "Success"
            if discovery_status in ("Error", "Network Error", "Timeout"):
                overall = f"Error"
            elif which_path not in ("Sitemap", "CSS", "Both"):
                overall = "Error"
            elif cleaning_status != "Success":
                overall = "Error"

            updates["Overall pipelines Status"] = overall
            updates["Domain (sources)"] = key
            # Friendly cleaning sentence if nothing kept
            try:
                kept = int(cnt.get("kept", 0))
                dup = int(cnt.get("dup", 0))
                no_date = int(cnt.get("no_date", 0))
                out_day = int(cnt.get("out_day", 0))
                total = kept + dup + no_date + out_day
                if total > 0 and kept == 0:
                    if dup == total:
                        cmsg = "Cleaning: all items were duplicates."
                    elif no_date == total:
                        cmsg = "Cleaning: all items were missing dates."
                    elif out_day == total:
                        cmsg = "Cleaning: all items were outside today’s date."
                    else:
                        cmsg = "Cleaning: kept 0 after filters."
                    updates["Overall pipelines Explanation"] = cmsg
            except Exception:
                pass
            try:
                ov_upsert(key, updates)
            except Exception:
                pass
    except Exception:
        pass

    # Final CSV Export from SQLite
    try:
        ov_export_csv()
        print("[clean] Exported pipelines_overview.csv from SQLite store")
    except Exception:
        print("[clean] Warning: failed to export CSV from SQLite store")

    return summary


# =============================
# Selector-assisted (optional)
# =============================
def clean_from_stream(
    stream_path: str = "selection_extraction_report_stream.jsonl",
    out_clean_path: str = "articles_clean_current.jsonl",
    out_removed_no_date_path: str = "articles_removed_no_date.jsonl",
    out_removed_out_of_current_day_path: str = "articles_removed_out_of_current_day.jsonl",
    out_removed_duplicate_path: str = "articles_removed_duplicate.jsonl",
    out_removed_no_title_path: str = "articles_removed_no_title.jsonl",
    out_summary_path: str = "articles_cleaning_summary.json",
    per_sitemap_timeout: float = 15.0,
    sitemap_workers: int = 4,
) -> Dict[str, int]:
    """Main cleaning routine.

    Returns summary dict with counts.
    """
    total_input = 0
    kept_count = 0
    removed_no_date = 0
    removed_not_today = 0
    removed_duplicate = 0
    removed_no_title = 0

    # Prepare outputs
    try:
        if os.path.exists(out_clean_path):
            os.remove(out_clean_path)
        if os.path.exists(out_removed_no_date_path):
            os.remove(out_removed_no_date_path)
        if os.path.exists(out_removed_out_of_current_day_path):
            os.remove(out_removed_out_of_current_day_path)
        if os.path.exists(out_removed_duplicate_path):
            os.remove(out_removed_duplicate_path)
        if os.path.exists(out_removed_no_title_path):
            os.remove(out_removed_no_title_path)
    except Exception:
        pass

    sitemaps, fields_map = _collect_sitemap_fields(stream_path)
    # Per-site aggregation for CSV (site_domain -> counters)
    per_site: Dict[str, Dict[str, int]] = {}
    seen_urls = set()

    start_perf = time.perf_counter()
    with open(out_clean_path, "a", encoding="utf-8") as f_keep, \
         open(out_removed_no_date_path, "a", encoding="utf-8") as f_no_date, \
         open(out_removed_out_of_current_day_path, "a", encoding="utf-8") as f_out_day, \
         open(out_removed_duplicate_path, "a", encoding="utf-8") as f_dup, \
         open(out_removed_no_title_path, "a", encoding="utf-8") as f_no_title:
        # Parallelize sitemap expansion to overlap network I/O
        max_workers = max(1, int(sitemap_workers))
        jobs: List[cf.Future] = []
        with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
            for sm_url in sitemaps:
                jobs.append(ex.submit(_expand_entries_for_sitemap, sm_url, fields_map, per_sitemap_timeout))

            for fut in cf.as_completed(jobs):
                try:
                    entries = fut.result() or []
                except Exception:
                    entries = []
                for e in entries:
                    if not isinstance(e, dict):
                        continue
                    total_input += 1
                    raw_url = e.get("url") or ""
                    # Prefer selector date when available, else fallback
                    raw_date = e.get("date") or e.get("lastmod") or ""
                    sm_url = e.get("sourceSitemap") or ""

                    canonical_url = _canonicalize_url(raw_url)
                    domain = _domain_from_url(canonical_url)
                    # Group by original site domain from source sitemap
                    site_domain = _domain_from_url(e.get("sourceSitemap") or "")
                    if site_domain and site_domain not in per_site:
                        per_site[site_domain] = {"kept": 0, "dup": 0, "no_date": 0, "out_day": 0, "no_title": 0}

                    # Deduplicate first (cheapest)
                    if canonical_url in seen_urls:
                        removed_duplicate += 1
                        f_dup.write(json.dumps({
                            "url": raw_url,
                            "canonicalUrl": canonical_url,
                            "domain": domain,
                            "dateOriginal": raw_date,
                            "reason": "duplicate_url",
                            "duplicateOf": canonical_url,
                            "sourceSitemap": sm_url,
                        }, ensure_ascii=False) + "\n")
                        if site_domain:
                            per_site[site_domain]["dup"] += 1
                        continue

                    # Date presence check (no parsing yet)
                    if not (isinstance(raw_date, str) and raw_date.strip()):
                        removed_no_date += 1
                        f_no_date.write(json.dumps({
                            "url": raw_url,
                            "canonicalUrl": canonical_url,
                            "domain": domain,
                            "dateOriginal": raw_date,
                            "reason": "no_date",
                            "sourceSitemap": sm_url,
                        }, ensure_ascii=False) + "\n")
                        if site_domain:
                            per_site[site_domain]["no_date"] += 1
                        continue

                    # Parse date (fast ISO -> dateutil)
                    dt = _parse_date_any(raw_date)
                    if dt is None:
                        removed_no_date += 1
                        f_no_date.write(json.dumps({
                            "url": raw_url,
                            "canonicalUrl": canonical_url,
                            "domain": domain,
                            "dateOriginal": raw_date,
                            "reason": "no_date",
                            "sourceSitemap": sm_url,
                        }, ensure_ascii=False) + "\n")
                        if site_domain:
                            per_site[site_domain]["no_date"] += 1
                        continue

                    # Same local day filter
                    if not _is_same_local_day(dt):
                        removed_not_today += 1
                        f_out_day.write(json.dumps({
                            "url": raw_url,
                            "canonicalUrl": canonical_url,
                            "domain": domain,
                            "dateOriginal": raw_date,
                            "dateParsedISO": dt.astimezone(timezone.utc).isoformat(),
                            "reason": "out_of_current_day",
                            "sourceSitemap": sm_url,
                        }, ensure_ascii=False) + "\n")
                        if site_domain:
                            per_site[site_domain]["out_day"] += 1
                        continue

                    # Require explicit title; filter if missing
                    title = e.get("title")
                    if not (isinstance(title, str) and title.strip()):
                        removed_no_title += 1
                        f_no_title.write(json.dumps({
                            "url": raw_url,
                            "canonicalUrl": canonical_url,
                            "domain": domain,
                            "dateOriginal": raw_date,
                            "reason": "no_title",
                            "sourceSitemap": sm_url,
                        }, ensure_ascii=False) + "\n")
                        if site_domain:
                            per_site[site_domain]["no_title"] += 1
                        continue

                    # Keep and enrich
                    seen_urls.add(canonical_url)
                    kept_count += 1

                    summary = _build_summary_from_fields(e) or ""
                    source = _source_name_from_domain(domain)

                    f_keep.write(json.dumps({
                        "title": title,
                        "url": raw_url,
                        "summary": summary,
                        "date": dt.astimezone(timezone.utc).isoformat(),
                        "source": source,
                    }, ensure_ascii=False) + "\n")
                    if site_domain:
                        per_site[site_domain]["kept"] += 1

    end_perf = time.perf_counter()
    summary = {
        "totalEntries": total_input,
        "kept": kept_count,
        "removed_no_date": removed_no_date,
        "removed_out_of_current_day": removed_not_today,
        "removed_duplicate_url": removed_duplicate,
        "removed_no_title": removed_no_title,
        "totalDurationSec": round(float(end_perf - start_perf), 3),
        "uniqueLeafSitemaps": len(sitemaps),
        "runAtUtc": datetime.utcnow().isoformat() + "Z",
    }

    try:
        with open(out_summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # === CSV upsert per-site: cleaning + overall ===
    try:
        rows = _read_csv_map(_CSV_PATH)
        # Build domain -> existing CSV key map (prefer longest key per host)
        domain_to_key: Dict[str, str] = {}
        try:
            for k in rows.keys():
                host = _domain_from_url(k)
                h = (host or "").lower()
                if h.startswith("www."):
                    h = h[4:]
                if not h:
                    continue
                prev = domain_to_key.get(h)
                if (prev is None) or (len(k) > len(prev)):
                    domain_to_key[h] = k
        except Exception:
            domain_to_key = {}
        for domain, cnt in per_site.items():
            updates: Dict[str, str] = {
                "Domain (sources)": domain,
                "Cleaning Status": "Success",
                "Cleaned Articles (Final)": str(int(cnt.get("kept", 0))),
                "Duplicates Removed": str(int(cnt.get("dup", 0))),
                "Missing Dates Removed": str(int(cnt.get("no_date", 0))),
                "Missing Titles Removed": str(int(cnt.get("no_title", 0))),
                "Out of Range/Old Date Removed": str(int(cnt.get("out_day", 0))),
            }
            # Build cleaning error segment (only if nothing kept)
            try:
                kept = int(cnt.get("kept", 0))
                dup = int(cnt.get("dup", 0))
                no_date = int(cnt.get("no_date", 0))
                out_day = int(cnt.get("out_day", 0))
                total = kept + dup + no_date + out_day
                if total > 0 and kept == 0:
                    if dup == total:
                        creason = "all_duplicates"
                    elif no_date == total:
                        creason = "all_no_date"
                    elif out_day == total:
                        creason = "all_out_of_current_day"
                    else:
                        creason = "kept_zero"
                    cctx = f"kept={kept} dup={dup} no_date={no_date} out_day={out_day}"
                    updates["Overall pipelines Error Details"] = f"cleaning: {creason}; {cctx}"
            except Exception:
                pass
            # Map domain -> existing URL-key row; skip if none
            host = (domain or "").lower()
            if host.startswith("www."):
                host_base = host[4:]
            else:
                host_base = host
            key = domain_to_key.get(host_base) or domain_to_key.get("www." + host_base)
            if not key:
                # No matching existing row; avoid creating a new domain-only row
                continue
            row = rows.get(key) or _default_row(key)
            discovery_status = (row.get("Sitemap Processing Status") or "").strip()
            which_path = (row.get("Which Path Used for Final Extraction") or "").strip()
            overall = "Success"
            if discovery_status in ("Error", "Network Error", "Timeout"):
                overall = "Error"
            elif which_path not in ("Sitemap", "CSS", "Both"):
                overall = "Error"
            updates["Overall pipelines Status"] = overall
            # Friendly cleaning sentence if nothing kept
            try:
                kept = int(cnt.get("kept", 0))
                dup = int(cnt.get("dup", 0))
                no_date = int(cnt.get("no_date", 0))
                out_day = int(cnt.get("out_day", 0))
                total = kept + dup + no_date + out_day
                if total > 0 and kept == 0:
                    if dup == total:
                        cmsg = "Cleaning: all items were duplicates."
                    elif no_date == total:
                        cmsg = "Cleaning: all items were missing dates."
                    elif out_day == total:
                        cmsg = "Cleaning: all items were outside today’s date."
                    else:
                        cmsg = "Cleaning: kept 0 after filters."
                    updates["Overall pipelines Explanation"] = cmsg
            except Exception:
                pass
            _upsert_csv_row(key, updates)
    except Exception:
        pass

    return summary


def main() -> None:
    # Default paths are relative to python_tools directory
    base_dir = os.path.dirname(__file__) or "."
    stream_path = os.path.join(base_dir, "selection_extraction_report_stream.jsonl")
    stream_articles = os.path.join(base_dir, "stream_scraped_articles.jsonl")
    out_clean = os.path.join(base_dir, "articles_clean_current.jsonl")
    out_removed_no_date = os.path.join(base_dir, "articles_removed_no_date.jsonl")
    out_removed_out_day = os.path.join(base_dir, "articles_removed_out_of_current_day.jsonl")
    out_removed_dup = os.path.join(base_dir, "articles_removed_duplicate.jsonl")
    out_removed_no_title = os.path.join(base_dir, "articles_removed_no_title.jsonl")
    out_summary = os.path.join(base_dir, "articles_cleaning_summary.json")

    # OFFLINE by default: clean local stream without any network
    summary = clean_offline_from_streamed_articles(
        input_path=stream_articles,
        out_clean_path=out_clean,
        out_removed_no_date_path=out_removed_no_date,
        out_removed_out_of_current_day_path=out_removed_out_day,
        out_removed_duplicate_path=out_removed_dup,
        out_removed_no_title_path=out_removed_no_title,
        out_summary_path=out_summary,
    )
    # To use selector-assisted mode instead, call clean_from_stream(...)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


