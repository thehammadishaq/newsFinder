import os
import io
import re
import gzip
import time
import httpx
import random
import hashlib
from urllib.parse import urlparse, urljoin
from typing import List, Optional
from xml.etree import ElementTree as ET
# Robot retry diagnostics (last call snapshot)
ROBOTS_RETRY_LAST = {"attempted": False, "status": "not_attempted"}


def _ensure_debug_dirs() -> None:
    os.makedirs("debug_html", exist_ok=True)
def _clickable_path(path: str) -> str:
    try:
        from pathlib import Path
        return Path(os.path.abspath(path)).as_uri()
    except Exception:
        try:
            return os.path.abspath(path)
        except Exception:
            return str(path)


def _random_user_agent() -> str:
    chrome_versions = [
        "127.0.6533.72",
        "128.0.6613.84",
        "129.0.6668.90",
    ]
    version = random.choice(chrome_versions)
    platforms = [
        "Windows NT 10.0; Win64; x64",
        "Macintosh; Intel Mac OS X 10_15_7",
        "X11; Linux x86_64",
    ]
    platform = random.choice(platforms)
    return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"


def _normalize_root_url(root_url: str) -> str:
    try:
        p = urlparse(root_url)
        if not p.scheme:
            return "https://" + root_url.lstrip("/")
        return root_url
    except Exception:
        return root_url


def fetch_robots_txt_meta(root_url: str, timeout: float = 60000.0) -> dict:
    """Fetch robots.txt and return meta.

    Returns a dict: {"text": str, "attempted": bool, "status": str, "url_used": str}
    where status is one of: not_attempted | not_needed | bypassed | failed | disabled
    """
    global ROBOTS_RETRY_LAST
    ROBOTS_RETRY_LAST = {"attempted": False, "status": "not_attempted"}
    _ensure_debug_dirs()
    url = _normalize_root_url(root_url)
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = urljoin(base, "/robots.txt")
    print(f"[sitemap] Fetching robots.txt -> {robots_url}")
    headers = {"User-Agent": _random_user_agent(), "Accept": "text/plain,*/*;q=0.8"}
    result = {"text": "", "attempted": False, "status": "not_attempted", "url_used": robots_url}
    try:
        with httpx.Client(timeout=timeout, headers=headers, follow_redirects=False) as client:
            r = client.get(robots_url)
            status_code = int(getattr(r, "status_code", 0) or 0)
            if (300 <= status_code < 400) or (status_code == 403):
                retry_enabled = str(os.getenv("ROBOTS_BROWSER_RETRY", "1")).strip().lower() in ("1", "true", "yes", "on")
                if retry_enabled:
                    print(f"[sitemap] robots.txt returned {status_code}; retrying via browser (once)")
                    try:
                        from playwright.sync_api import sync_playwright  # type: ignore
                        ROBOTS_RETRY_LAST["attempted"] = True
                        ROBOTS_RETRY_LAST["status"] = "failed"
                        with sync_playwright() as p:
                            proxy_server = os.getenv("PROXY_SERVER")
                            proxy_kw = {"server": proxy_server} if (proxy_server and proxy_server.strip()) else None
                            if proxy_kw:
                                try:
                                    _pp = urlparse(proxy_server)
                                    _phost = _pp.hostname or "?"
                                    _pport = _pp.port or "?"
                                    print(f"[sitemap] robots.txt browser retry using proxy {_pp.scheme}://{_phost}:{_pport}")
                                except Exception:
                                    print("[sitemap] robots.txt browser retry using proxy (configured)")
                            browser = p.chromium.launch(headless=True, proxy=proxy_kw) if proxy_kw else p.chromium.launch(headless=True)
                            context = browser.new_context(
                                user_agent=(
                                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                                ),
                                viewport={"width": 1366, "height": 768},
                                locale="en-US",
                                timezone_id="Australia/Sydney",
                            )
                            page = context.new_page()
                            try:
                                page.goto(robots_url, wait_until="networkidle", timeout=60000)
                                body_text = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
                            finally:
                                try:
                                    browser.close()
                                except Exception:
                                    pass
                        text = str(body_text or "")
                        if text.strip():
                            ROBOTS_RETRY_LAST["status"] = "bypassed"
                            result.update({"text": text, "attempted": True, "status": "bypassed"})
                            ts = time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())
                            fname = f"robots_{parsed.netloc}_{ts}.txt"
                            try:
                                with open(os.path.join("debug_html", fname), "w", encoding="utf-8") as f:
                                    f.write(text)
                            except Exception:
                                pass
                            print(f"[sitemap] robots.txt (browser) saved to {_clickable_path(os.path.join('debug_html', fname))} (chars={len(text)})")
                            return result
                        else:
                            print("[sitemap] robots.txt (browser) empty response")
                    except Exception as _be:
                        print(f"[sitemap] robots.txt browser retry failed: {type(_be).__name__}")
                else:
                    ROBOTS_RETRY_LAST["status"] = "disabled"
                    print(f"[sitemap] robots.txt returned {status_code}; browser retry disabled")
                # Fail case
                result.update({"text": "", "attempted": bool(ROBOTS_RETRY_LAST.get("attempted")), "status": str(ROBOTS_RETRY_LAST.get("status") or "failed")})
                return result
            if status_code >= 400:
                print(f"[sitemap] robots.txt not accessible (status={r.status_code})")
                result.update({"text": "", "attempted": False, "status": "failed"})
                return result
            text = r.text or ""
            result.update({"text": text, "attempted": False, "status": "not_needed"})
    except Exception:
        print("[sitemap] robots.txt fetch failed (exception)")
        result.update({"text": "", "attempted": False, "status": "failed"})
        return result

    ts = time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())
    fname = f"robots_{parsed.netloc}_{ts}.txt"
    try:
        with open(os.path.join("debug_html", fname), "w", encoding="utf-8") as f:
            f.write(result["text"])
    except Exception:
        pass
    print(f"[sitemap] robots.txt saved to {_clickable_path(os.path.join('debug_html', fname))} (chars={len(result['text'])})")
    return result


def fetch_robots_txt(root_url: str, timeout: float = 60000.0) -> Optional[str]:
    meta = fetch_robots_txt_meta(root_url, timeout=timeout)
    return meta.get("text") or None


def parse_sitemaps_from_robots(robots_txt: str, base_url: str, news_only: bool = True) -> List[str]:
    urls: List[str] = []
    if not robots_txt:
        return urls
    base = _normalize_root_url(base_url)
    # Match lines like: Sitemap: https://example.com/sitemap.xml
    for line in robots_txt.splitlines():
        if line.lower().startswith("sitemap:"):
            raw = line.split(":", 1)[1].strip()
            if not raw:
                continue
            try:
                abs_url = urljoin(base, raw)
            except Exception:
                abs_url = raw
            urls.append(abs_url)
    # de-duplicate while preserving order
    seen = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    print(f"[sitemap] Found {len(out)} sitemap URL(s) in robots.txt")
    # Filter for news-related sitemaps if requested
    if news_only:
        filtered: List[str] = []
        for u in out:
            ul = (u or "").lower()
            # Include if contains news, article, post, or looks like main sitemap
            if any(kw in ul for kw in ["news", "article", "post", "sitemap.xml", "sitemap_index"]):
                filtered.append(u)
        print(f"[sitemap] After news-only filter: {len(filtered)} sitemap URL(s)")
        return filtered
    
    # No filtering - return all sitemaps as-is
    print(f"[sitemap] Returning {len(out)} sitemap(s) without filtering")
    return out


def _parse_xml_bytes(data: bytes) -> Optional[ET.Element]:
    try:
        return ET.fromstring(data)
    except Exception:
        return None


def _fetch_bytes(url: str, timeout: float) -> Optional[bytes]:
    headers = {"User-Agent": _random_user_agent(), "Accept": "application/xml,text/xml,*/*;q=0.8"}
    try:
        with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
                r = client.get(url)
                if r.status_code >= 400:
                    return None
                return r.content
    except Exception:
        return None


def _maybe_decompress(url: str, content: Optional[bytes]) -> Optional[bytes]:
    if content is None:
        return None
    if url.lower().endswith(".gz"):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                return gz.read()
        except Exception:
            return None
    return content


def expand_sitemap_all(sitemap_url: str, timeout: float = 15.0, max_urls: int = 0) -> List[str]:
    # No heuristics: collect ALL URLs recursively from sitemapindex/urlset until max_urls
    urls_out: List[str] = []

    def visit(url: str) -> None:
        if max_urls > 0 and len(urls_out) >= max_urls:
            return
        print(f"[sitemap] Expanding: {url}")
        raw = _fetch_bytes(url, timeout)
        raw = _maybe_decompress(url, raw)
        if not raw:
            print(f"[sitemap] Skip (no content): {url}")
            return
        root = _parse_xml_bytes(raw)
        if root is None:
            print(f"[sitemap] Skip (invalid XML): {url}")
            return
        tag = (root.tag or "").lower()
        if tag.endswith("sitemapindex"):
            for sm in root.findall(".//{*}sitemap"):
                loc = sm.findtext("{*}loc") or sm.findtext("loc")
                if not loc:
                    continue
                visit(loc.strip())
                if max_urls > 0 and len(urls_out) >= max_urls:
                    break
        elif tag.endswith("urlset"):
            for u in root.findall(".//{*}url"):
                loc = u.findtext("{*}loc") or u.findtext("loc")
                if not loc:
                    continue
                urls_out.append(loc.strip())
                if max_urls > 0 and len(urls_out) >= max_urls:
                    break
        else:
            # Try best-effort namespace-agnostic
            # Some providers omit standard tags; attempt both
            for sm in root.findall(".//sitemap"):
                loc = None
                for child in list(sm):
                    if child.tag.endswith("loc"):
                        loc = (child.text or "").strip()
                        break
                if loc:
                    visit(loc)
                    if max_urls > 0 and len(urls_out) >= max_urls:
                        break
            if not urls_out:
                for u in root.findall(".//url"):
                    loc = None
                    for child in list(u):
                        if child.tag.endswith("loc"):
                            loc = (child.text or "").strip()
                            break
                    if loc:
                        urls_out.append(loc)
                        if max_urls > 0 and len(urls_out) >= max_urls:
                            break

    visit(sitemap_url)
    # de-duplicate preserving order
    seen = set()
    deduped: List[str] = []
    for u in urls_out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    limit = max_urls if max_urls > 0 else len(deduped)
    print(f"[sitemap] Expanded {sitemap_url} -> collected {len(deduped[:limit])} URL(s)")
    return deduped[:limit]


def gather_all_urls_from_robots(root_url: str, timeout: float = 15.0, max_urls: int = 100000) -> List[str]:
    print(f"[sitemap] Gathering URLs via robots.txt for: {root_url}")
    robots = fetch_robots_txt(root_url, timeout=timeout)
    if not robots:
        print("[sitemap] No robots.txt available; returning empty URL list")
        return []
    sitemaps = parse_sitemaps_from_robots(robots, root_url)
    if not sitemaps:
        print("[sitemap] No sitemap entries found in robots.txt")
        return []
    all_urls: List[str] = []
    for sm in sitemaps:
        print(f"[sitemap] Processing sitemap: {sm}")
        urls = expand_sitemap_all(sm, timeout=timeout, max_urls=max_urls - len(all_urls))
        if urls:
            all_urls.extend(urls)
        if len(all_urls) >= max_urls:
            break
    # de-duplicate preserving order
    seen = set()
    out: List[str] = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    # Write debug
    try:
        parsed = urlparse(_normalize_root_url(root_url))
        ts = time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())
        path = os.path.join("debug_html", f"{parsed.netloc}_{ts}_SITEMAP_URLS.json")
        with open(path, "w", encoding="utf-8") as f:
            import json as _json
            _json.dump({"domain": parsed.netloc, "total": len(out), "urls": out}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    print(f"[sitemap] Total URLs collected from all sitemaps: {len(out)}")
    return out


# ======================
# Date-aware entry export
# ======================

def _parse_w3c_datetime(value: str) -> Optional[object]:
    """Parse W3C datetime (ISO 8601) or date-only string to datetime object."""
    from datetime import datetime, timezone
    if not value:
        return None
    s = value.strip()
    try:
        # Normalize Z to +00:00 for fromisoformat
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # If date-only, fromisoformat handles YYYY-MM-DD
        dt = datetime.fromisoformat(s)
        # Assume naive datetime is UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        # Try loose date-only
        try:
            dt = datetime.strptime(value[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None


def _child_text_any_ns(parent: ET.Element, local_name: str) -> Optional[str]:
    for child in list(parent):
        if (child.tag or "").endswith(local_name):
            txt = (child.text or "").strip()
            if txt:
                return txt
    return None


def _extract_date_from_url_element(u: ET.Element) -> Optional[str]:
    # Prefer news:publication_date, then lastmod
    # 1) Search any descendant tag *publication_date
    for desc in u.iter():
        if (desc.tag or "").endswith("publication_date"):
            val = (desc.text or "").strip()
            if val:
                return val
    # 2) Fallback to direct lastmod child
    lastmod = _child_text_any_ns(u, "lastmod")
    if lastmod:
        return lastmod
    return None


def expand_sitemap_entries_all(sitemap_url: str, timeout: float = 15.0, max_urls: int = 0) -> List[dict]:
    """Collect entries with url+date (skip old dated child sitemaps)."""
    import re
    entries: List[dict] = []
    consecutive_empty_count = [0]  # Use list to allow mutation in nested function

    def visit(url: str) -> None:
        if max_urls > 0 and len(entries) >= max_urls:
            return
        print(f"[sitemap] Expanding (entries): {url}")
        raw = _fetch_bytes(url, timeout)
        raw = _maybe_decompress(url, raw)
        if not raw:
            print(f"[sitemap] Skip (no content): {url}")
            return
        root = _parse_xml_bytes(raw)
        if root is None:
            print(f"[sitemap] Skip (invalid XML): {url}")
            return
        tag = (root.tag or "").lower()
        if tag.endswith("sitemapindex"):
            for sm in root.findall(".//{*}sitemap"):
                # Early exit if too many consecutive empty sitemaps (e.g., qz.com has 256 empty ones)
                if consecutive_empty_count[0] >= 10:
                    print(f"[sitemap] Stopping: {consecutive_empty_count[0]} consecutive empty sitemaps")
                    break
                loc = _child_text_any_ns(sm, "loc")
                if not loc:
                    continue
                loc_s = (loc or "").strip()
                entries_before = len(entries)
                visit(loc_s)
                entries_after = len(entries)
                # Track consecutive empty results
                if entries_after == entries_before:
                    consecutive_empty_count[0] += 1
                else:
                    consecutive_empty_count[0] = 0  # Reset on success
                if max_urls > 0 and len(entries) >= max_urls:
                    break
        elif tag.endswith("urlset"):
            for u in root.findall(".//{*}url"):
                loc = _child_text_any_ns(u, "loc")
                if not loc:
                    continue
                date = _extract_date_from_url_element(u)
                entries.append({"url": loc.strip(), "date": date})
                if max_urls > 0 and len(entries) >= max_urls:
                    break
        else:
            # Best-effort namespace-agnostic handling
            for sm in root.findall(".//sitemap"):
                loc = _child_text_any_ns(sm, "loc")
                if loc:
                    visit(loc)
                    if max_urls > 0 and len(entries) >= max_urls:
                        break
            if not entries:
                for u in root.findall(".//url"):
                    # try to read url+date
                    loc = _child_text_any_ns(u, "loc")
                    if not loc:
                        continue
                    date = _extract_date_from_url_element(u)
                    entries.append({"url": (loc or "").strip(), "date": date})
                    if len(entries) >= max_urls:
                        break

    visit(sitemap_url)
    # de-duplicate by url preserving order
    seen = set()
    deduped: List[dict] = []
    for e in entries:
        u = e.get("url") or ""
        if u and u not in seen:
            seen.add(u)
            deduped.append(e)
    limit = max_urls if max_urls > 0 else len(deduped)
    print(f"[sitemap] Expanded (entries) {sitemap_url} -> collected {len(deduped[:limit])} item(s)")
    return deduped[:limit]


def gather_all_entries_from_robots(root_url: str, timeout: float = 15.0, max_urls: int = 100000) -> List[dict]:
    print(f"[sitemap] Gathering entries via robots.txt for: {root_url}")
    robots = fetch_robots_txt(root_url, timeout=timeout)
    if not robots:
        print("[sitemap] No robots.txt available; returning empty entries list")
        return []
    sitemaps = parse_sitemaps_from_robots(robots, root_url)
    if not sitemaps:
        print("[sitemap] No sitemap entries found in robots.txt")
        return []
    entries: List[dict] = []
    for sm in sitemaps:
        print(f"[sitemap] Processing sitemap (entries): {sm}")
        part = expand_sitemap_entries_all(sm, timeout=timeout, max_urls=max_urls - len(entries))
        if part:
            entries.extend(part)
        if len(entries) >= max_urls:
            break
    # de-duplicate by url preserving order
    seen = set()
    out: List[dict] = []
    for e in entries:
        u = e.get("url") or ""
        if u and u not in seen:
            seen.add(u)
            out.append(e)
    # Write debug
    try:
        parsed = urlparse(_normalize_root_url(root_url))
        ts = time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())
        path = os.path.join("debug_html", f"{parsed.netloc}_{ts}_SITEMAP_ENTRIES.json")
        with open(path, "w", encoding="utf-8") as f:
            import json as _json
            _json.dump({"domain": parsed.netloc, "total": len(out), "entries": out}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    print(f"[sitemap] Total entries collected from all sitemaps: {len(out)}")
    return out


# ======================
# Latest-updated sitemap and recent window filter
# ======================

def _extract_field_by_xpath(element, xpath_str: str) -> Optional[str]:
    """Extract field value using XPath-style path (e.g., 'news:news/news:title')."""
    if not xpath_str or element is None:
        return None
    
    try:
        # Handle direct children (e.g., 'loc', 'lastmod')
        if '/' not in xpath_str:
            # Strip namespace prefix if present (e.g., 'ns0:lastmod' -> 'lastmod')
            local = xpath_str.split(':')[-1] if ':' in xpath_str else xpath_str
            return _child_text_any_ns(element, local)
        
        # Handle nested paths (e.g., 'news:news/news:title')
        parts = xpath_str.split('/')
        current = element
        
        for part in parts:
            if current is None:
                return None
            # Strip namespace prefix for search (news:title -> title)
            tag_name = part.split(':')[-1] if ':' in part else part
            # Use namespace-agnostic search (find first child with matching local name)
            found = None
            for child in current:
                local_name = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                if local_name == tag_name:
                    found = child
                    break
            current = found
        
        return (current.text or "").strip() if current is not None else None
    except Exception as ex:
        return None


def expand_sitemap_entries_recent(
    sitemap_url: str,
    recent_hours: Optional[int] = None,
    timeout: float = 15.0,
    max_urls: int = 0,
    field_selectors: Optional[dict] = None,
) -> List[dict]:
    """Expand entries from a sitemap URL.

    - If recent_hours is None or 0: Extract ALL articles (no date filtering)
    - If recent_hours > 0: Only expand recent entries
    - If it's a sitemapindex, visit only child sitemaps that are recent by lastmod
      (>= cutoff) or, if lastmod is missing, whose first 10 entries include a recent date.
    - If it's a urlset, include only entries whose date >= cutoff.
    """
    from datetime import datetime, timezone, timedelta
    
    # None or 0 means NO FILTERING
    if recent_hours is None or recent_hours == 0:
        cutoff = None
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(recent_hours)))
    collected: List[dict] = []
    seen = set()

    raw = _fetch_bytes(sitemap_url, timeout)
    raw = _maybe_decompress(sitemap_url, raw)
    if not raw:
        return []
    root = _parse_xml_bytes(raw)
    if root is None:
        return []
    tag = (root.tag or "").lower()

    def add_entry(e: dict) -> None:
        if max_urls > 0 and len(collected) >= max_urls:
            return
        u = e.get("url") or ""
        if not u:
            return
        if u in seen:
            return
        seen.add(u)
        collected.append(e)

    if tag.endswith("sitemapindex"):
        for sm in root.findall(".//{*}sitemap"):
            loc = _child_text_any_ns(sm, "loc")
            if not loc:
                continue
            loc_s = (loc or "").strip()
            
            # If no cutoff (no filtering), include all child sitemaps
            if cutoff is None:
                include = True
            else:
                # With filtering: check if child sitemap is recent
                lm = _child_text_any_ns(sm, "lastmod")
                include = False
                if lm:
                    dt = _parse_w3c_datetime(lm)
                    include = bool(dt and dt >= cutoff)
                else:
                    # Peek first 10 entries
                    peek = expand_sitemap_entries_all(loc_s, timeout=timeout, max_urls=10)
                    for e in peek:
                        dts = e.get("date") or ""
                        edt = _parse_w3c_datetime(dts)
                        if edt and edt >= cutoff:
                            include = True
                            break
            if not include:
                continue
            # Recurse into child with recent-only expansion
            child_entries = expand_sitemap_entries_recent(loc_s, recent_hours=recent_hours, timeout=timeout, max_urls=max(0, max_urls - len(collected)), field_selectors=field_selectors)
            for e in child_entries:
                add_entry(e)
            if max_urls > 0 and len(collected) >= max_urls:
                break
    elif tag.endswith("urlset"):
        for u in root.findall(".//{*}url"):
            loc = _child_text_any_ns(u, "loc")
            if not loc:
                continue
            date_str = _extract_date_from_url_element(u)
            dt = _parse_w3c_datetime(date_str or "") if date_str else None
            
            # If no cutoff (no filtering), include all entries
            # Otherwise, only include if date >= cutoff
            if cutoff is None or (dt and dt >= cutoff):
                # Dynamic field extraction based on detected selectors
                if field_selectors and isinstance(field_selectors, dict):
                    entry = {}
                    for field_name, xpath in field_selectors.items():
                        value = _extract_field_by_xpath(u, xpath)
                        if value:
                            entry[field_name] = value
                    # Ensure 'url' field is present (fallback to loc)
                    if "url" not in entry and loc:
                        entry["url"] = loc.strip()
                    add_entry(entry)
                else:
                    # Fallback to default fields
                    chfreq = _child_text_any_ns(u, "changefreq")
                    priority = _child_text_any_ns(u, "priority")
                    add_entry({"url": loc.strip(), "date": date_str, "changefreq": chfreq, "priority": priority})
            if max_urls > 0 and len(collected) >= max_urls:
                break
    else:
        # Namespace-agnostic best-effort
        for sm in root.findall(".//sitemap"):
            loc = _child_text_any_ns(sm, "loc")
            if not loc:
                continue
            lm = _child_text_any_ns(sm, "lastmod")
            include = False
            if lm:
                dt = _parse_w3c_datetime(lm)
                include = bool(dt and dt >= cutoff)
            else:
                peek = expand_sitemap_entries_all(loc, timeout=timeout, max_urls=10)
                for e in peek:
                    dts = e.get("date") or ""
                    edt = _parse_w3c_datetime(dts)
                    if edt and edt >= cutoff:
                        include = True
                        break
            if not include:
                continue
            child_entries = expand_sitemap_entries_recent(loc, recent_hours=recent_hours, timeout=timeout, max_urls=max(0, max_urls - len(collected)))
            for e in child_entries:
                add_entry(e)
            if max_urls > 0 and len(collected) >= max_urls:
                break
    return collected


def gather_recent_entries_from_all_fresh_sitemaps(
    root_url: str,
    recent_hours: int = 24,
    timeout: float = 15.0,
    max_urls: int = 200000,
) -> List[dict]:
    from datetime import datetime, timezone, timedelta
    print(f"[sitemap] Aggregating recent entries across sitemaps with pre-check (last {recent_hours} hour(s))")
    robots = fetch_robots_txt(root_url, timeout=timeout)
    if not robots:
        print("[sitemap] No robots.txt; cannot list sitemaps")
        return []
    sitemaps = parse_sitemaps_from_robots(robots, root_url)
    if not sitemaps:
        print("[sitemap] No sitemaps listed in robots.txt")
        return []
    out: List[dict] = []
    seen = set()
    
    # Process each sitemap without filtering
    for sm in sitemaps:
        print(f"[sitemap] Processing sitemap: {sm}")
        
        # Expand recent-only (respecting max_urls budget)
        entries = expand_sitemap_entries_recent(sm, recent_hours=recent_hours, timeout=timeout, max_urls=max(0, max_urls - len(out)))
        for e in entries:
            u = e.get("url") or ""
            if u and u not in seen:
                seen.add(u)
                out.append(e)
        if len(out) >= max_urls:
            break
    print(f"[sitemap] Total recent entries across selected sitemaps: {len(out)}")
    return out


def _analyze_sitemap_freshness(sitemap_url: str, timeout: float = 15.0):
    """Return (newest_datetime, kind) from a sitemap."""
    from datetime import datetime, timezone
    raw = _fetch_bytes(sitemap_url, timeout)
    raw = _maybe_decompress(sitemap_url, raw)
    if not raw:
        return (None, "unknown")
    root = _parse_xml_bytes(raw)
    if root is None:
        return (None, "unknown")
    tag = (root.tag or "").lower()
    newest = None
    kind = "unknown"
    if tag.endswith("sitemapindex"):
        kind = "sitemapindex"
        for sm in root.findall(".//{*}sitemap"):
            lm = _child_text_any_ns(sm, "lastmod")
            dt = _parse_w3c_datetime(lm or "") if lm else None
            if dt and (newest is None or dt > newest):
                newest = dt
    elif tag.endswith("urlset"):
        kind = "urlset"
        for u in root.findall(".//{*}url"):
            lm = _child_text_any_ns(u, "lastmod")
            if not lm:
                # Try news:publication_date
                for desc in u.iter():
                    if (desc.tag or "").endswith("publication_date"):
                        lm = (desc.text or "").strip()
                        break
            dt = _parse_w3c_datetime(lm or "") if lm else None
            if dt and (newest is None or dt > newest):
                newest = dt
    return (newest, kind)


# Public aliases for helper utilities (for external import)
parse_w3c_datetime = _parse_w3c_datetime
fetch_bytes = _fetch_bytes
maybe_decompress = _maybe_decompress
parse_xml_bytes = _parse_xml_bytes
child_text_any_ns = _child_text_any_ns
