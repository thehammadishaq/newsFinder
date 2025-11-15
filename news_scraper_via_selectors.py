import os
import re
import json
import time
import random
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin
import concurrent.futures as cf
from typing import Dict, Any, List, Optional

from playwright.sync_api import sync_playwright
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore


def _ensure_dirs():
    os.makedirs("debug_html", exist_ok=True)


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


def _sanitize_html(html: str) -> str:
    html = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    html = re.sub(r" on[a-zA-Z]+=\"[^\"]*\"", "", html)
    html = re.sub(r" on[a-zA-Z]+='[^']*'", "", html)
    return html


def _snapshot_dom(page) -> str:
    html = page.evaluate("() => document.documentElement.outerHTML")
    return _sanitize_html(html or "")


def _readiness_loop(page, max_cycles: int = 6, sleep_ms: int = 250):
    last_text_len = 0
    last_links = 0
    for _ in range(max_cycles):
        page.evaluate("() => window.scrollBy(0, Math.floor(window.innerHeight * 0.8))")
        page.wait_for_timeout(sleep_ms)
        metrics = page.evaluate(
            "() => { const t = document.body?.innerText?.length || 0; const l = document.querySelectorAll('a').length; return { textLength: t, linkMatches: l }; }"
        )
        if not isinstance(metrics, dict):
            metrics = {"textLength": 0, "linkMatches": 0}
        if metrics["textLength"] <= last_text_len and metrics["linkMatches"] <= last_links:
            break
        last_text_len = metrics["textLength"]
        last_links = metrics["linkMatches"]


def _canonicalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        q = [(k, v) for k, v in parse_qsl(parsed.query) if not k.lower().startswith("utm_")]
        new_query = urlencode(q)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        return url


def _resolve_absolute(base_url: str, href: Optional[str]) -> str:
    try:
        return urljoin(base_url, href or "")
    except Exception:
        return href or ""


def _extract_text_attr(el, attr_list: List[str]) -> Optional[str]:
    if not el:
        return None
    for a in attr_list:
        v = el.getAttribute(a)
        if v and str(v).strip():
            return str(v).strip()
    text = el.textContent
    if text and str(text).strip():
        return str(text).strip()
    return None


def _evaluate_extraction(page, selectors: Dict[str, str], max_items: int) -> List[Dict[str, Any]]:
    # Runs in page context: builds article list using provided selectors only
    return page.evaluate(
        "({ sel, maxItems }) => {\n"
        "  function norm(s){ return (s||'').trim(); }\n"
        "  function selectClosest(base, sel){ if(!sel||!base) return null; let n = base; for(let d=0; d<4 && n; d++){ try{ const m = n.querySelector(sel); if(m) return m; }catch(e){} n = n.parentElement; } return null; }\n"
        "  function readDate(scope){ if(!scope) return null; const attrs=['datetime','content','data-time','data-date']; for(const a of attrs){ const v=scope.getAttribute(a); if(v&&v.trim()) return v.trim(); } const t=scope.textContent; if(t&&t.trim()) return t.trim(); return null; }\n"
        "  function readTicker(el){ if(!el) return null; const attrs=['alt','title','aria-label','data-symbol','data-ticker','data-qa-symbol']; for(const a of attrs){ const v=el.getAttribute(a); if(v&&v.trim()) return v.trim(); } const t=el.textContent; if(t&&t.trim()) return t.trim(); return null; }\n"
        "  const items = [];\n"
        "  const titleSel = sel.title; const linkSel = sel.link; if(!titleSel || !linkSel) return items;\n"
        "  const titleEls = Array.from(document.querySelectorAll(titleSel));\n"
        "  const linkEls = Array.from(document.querySelectorAll(linkSel));\n"
        "  const maxLen = Math.max(titleEls.length, linkEls.length);\n"
        "  for (let i = 0; i < maxLen && items.length < (maxItems||10000); i++) {\n"
        "    const t = titleEls[i] || null;\n"
        "    const a = linkEls[i] || null;\n"
        "    const scope = a?.closest('article, li, div, section') || t?.closest('article, li, div, section') || document;\n"
        "    let title = t ? t.textContent?.trim() : null;\n"
        "    let link = a ? a.getAttribute('href') : null;\n"
        "    if(!link && a){ const anchor = a.querySelector('a'); if(anchor) link = anchor.getAttribute('href'); }\n"
        "    if(!title && t){ const at = t.querySelector('a'); if(at) title = at.textContent?.trim(); }\n"
        "    if(!title && a){ title = a.textContent?.trim(); }\n"
        "    if(!title || !link) continue;\n"
        "    const out = { title, link };\n"
        "    if(sel.description){ try{ const d = selectClosest(a||t, sel.description) || scope.querySelector(sel.description); if(d){ const dt = d.textContent?.trim(); if(dt) out.description = dt; } }catch(e){} }\n"
        "    if(sel.author){ try{ const b = selectClosest(a||t, sel.author) || scope.querySelector(sel.author); if(b){ const bt = b.textContent?.trim(); if(bt) out.author = bt; } }catch(e){} }\n"
        "    if(sel.category){ try{ const c = selectClosest(a||t, sel.category) || scope.querySelector(sel.category); if(c){ const ct = c.textContent?.trim(); if(ct) out.category = ct; } }catch(e){} }\n"
        "    if(sel.date){ try{ const de = selectClosest(a||t, sel.date) || scope.querySelector(sel.date) || scope.querySelector('time[datetime]'); if(de){ const dv = readDate(de); if(dv) out.date = dv; } }catch(e){} }\n"
        "    if(sel.ticker){ try{ const te = selectClosest(a||t, sel.ticker) || scope.querySelector(sel.ticker); if(te){ const tv = readTicker(te); if(tv){ const cleaned = tv.trim(); if(cleaned){ const maybe = cleaned.toUpperCase(); let arr = []; if(/^[A-Z]{1,6}$/.test(maybe) || /^\$[A-Z]{1,6}$/.test(maybe) || /^(NYSE|NASDAQ|LON|EURONEXT|HKEX|TSE|KRX):[A-Z0-9.-]+$/i.test(cleaned)){ arr = [cleaned]; } else { const firstTok = cleaned.split(/\s+/)[0]; if(firstTok && firstTok.length <= 8) arr = [firstTok.toUpperCase()]; } if(arr.length) out.tickers = arr; } } } }catch(e){} }\n"
        "    items.push(out);\n"
        "  }\n"
        "  return items;\n"
        "}",
        {"sel": selectors, "maxItems": (max_items or 10000)}
    )


def extract_via_selectors(
    url: str,
    sections: List[Dict[str, Any]],
    headful: bool = False,
    slowmo_ms: int = 0,
    max_items: int = 10000,
) -> Dict[str, Any]:
    # Load .env once
    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            pass
    _ensure_dirs()
    print(f"[extract] Navigating to {url} (headful={headful}, slowmo={slowmo_ms})")
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.hostname or "unknown"
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%fZ")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful, slow_mo=slowmo_ms or None)
        context = browser.new_context(
            user_agent=_random_user_agent(),
            viewport={"width": random.randint(1200, 1440), "height": random.randint(800, 1000)},
            extra_http_headers={"Referer": f"https://{domain}/"},
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        _readiness_loop(page)

        html = _snapshot_dom(page)
        html_path = os.path.join("debug_html", f"{domain}_{ts}_HTML.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[extract] Snapshot saved: {html_path} (chars={len(html)})")

        all_articles: List[Dict[str, Any]] = []
        print(f"[extract] Sections to extract: {len(sections)}")
        for sec in sections:
            sel = sec.get("selectors") or {}
            if not isinstance(sel, dict):
                continue
            if not sel.get("title") or not sel.get("link"):
                continue
            try:
                items = _evaluate_extraction(page, sel, max_items)
            except Exception:
                items = []
            print(f"[extract] Section '{sec.get('sectionName') or 'Unnamed'}': items={len(items)}")
            for it in items:
                abs_link = _resolve_absolute(url, it.get("link", ""))
                it["link"] = _canonicalize_url(abs_link)
            all_articles.extend(items)

        browser.close()

    result = {
        "success": True,
        "domain": domain,
        "engine": "llm-selectors",
        "articles": all_articles[: max_items],
        "count": len(all_articles),
        "logs": {"html": html_path},
    }
    return result


def batch_extract_worker(url_in: str, sections_in: List[Dict[str, Any]], headful: bool, slowmo_ms: int, max_items: int) -> Dict[str, Any]:
    """Top-level worker for ProcessPoolExecutor (must be picklable on Windows)."""
    out = extract_via_selectors(
        url=url_in,
        sections=sections_in,
        headful=headful,
        slowmo_ms=slowmo_ms,
        max_items=max_items,
    )
    out["_input_url"] = url_in
    return out


def _cli():  # simple CLI
    import argparse

    parser = argparse.ArgumentParser(description="Extract articles via given selectors JSON")
    parser.add_argument("url", help="Target URL")
    parser.add_argument("--selectors", required=True, help="Path to selectors JSON (single-domain) or combined JSON (excel mode)")
    parser.add_argument("--for-url", help="When using combined selectors JSON, pick selectors for this URL (or domain)")
    parser.add_argument("--concurrency", type=int, default=1, help="Parallel workers when using combined selectors (default 1)")
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--slowmo", type=int, default=0)
    parser.add_argument("--max", dest="max_items", type=int, default=10000)
    parser.add_argument("--output", default="articles.json")
    args = parser.parse_args()

    # Resolve selectors path robustly so it works from project root or python_tools
    sel_path = args.selectors
    if not os.path.isabs(sel_path) and not os.path.exists(sel_path):
        script_dir = os.path.dirname(__file__)
        # If user passed 'python_tools/xyz.json' while running inside python_tools,
        # prefer the basename under script_dir
        cand_basename = os.path.join(script_dir, os.path.basename(sel_path))
        if os.path.exists(cand_basename):
            sel_path = cand_basename
        else:
            # Also try relative to project root (parent of python_tools)
            project_root = os.path.abspath(os.path.join(script_dir, ".."))
            cand_root = os.path.join(project_root, sel_path)
            if os.path.exists(cand_root):
                sel_path = cand_root

    with open(sel_path, "r", encoding="utf-8") as f:
        sel_json = json.load(f)

    # Support combined selectors JSON from Excel mode
    is_combined = isinstance(sel_json, dict) and "entries" in sel_json and isinstance(sel_json["entries"], list)
    sections: List[Dict[str, Any]] = []
    if is_combined and not args.for_url:
        # Batch extract for all entries in combined JSON
        entries = sel_json["entries"]
        if not entries:
            raise SystemExit("Combined selectors has no entries[]")
        script_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(script_dir, ".."))
        effective_headful = args.headful and (args.concurrency <= 1)
        if args.headful and args.concurrency > 1:
            print(f"[batch-extract] Headful disabled for concurrency={args.concurrency}. Set --concurrency 1 to view browser.")

        # Prepare tasks
        tasks: List[Dict[str, Any]] = []
        for e in entries:
            url_e = e.get("url") or ""
            domain_e = e.get("domain") or (urlparse(url_e).netloc if url_e else "")
            secs = e.get("sections") or []
            if not url_e or not secs:
                continue
            tasks.append({"url": url_e, "domain": domain_e, "sections": secs})
        if not tasks:
            raise SystemExit("No valid tasks found in combined selectors (missing url or sections)")

        print(f"[batch-extract] Tasks: {len(tasks)}; concurrency={max(1, int(args.concurrency))}")
        results: List[Dict[str, Any]] = []
        all_articles: List[Dict[str, Any]] = []
        combined_out: Dict[str, Any] = {"success": True, "mode": "combined", "processed": 0, "results": results, "articles": [], "total": 0}

        max_workers = max(1, int(args.concurrency))
        with cf.ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(
                    batch_extract_worker,
                    t["url"],
                    t["sections"],
                    effective_headful,
                    args.slowmo,
                    args.max_items,
                ): t
                for t in tasks
            }
            completed = 0
            for fut in cf.as_completed(future_to_task):
                t = future_to_task[fut]
                u = t["url"]
                try:
                    out = fut.result()
                    domain = (out.get("domain") or (urlparse(u).netloc or "unknown")).lower()
                    # Aggregate articles in-memory (no per-site files)
                    site_articles = out.get("articles") or []
                    if isinstance(site_articles, list):
                        all_articles.extend(site_articles)
                    results.append({
                        "url": u,
                        "domain": domain,
                        "success": True,
                        "count": out.get("count", 0),
                        "logs": out.get("logs") or {},
                    })
                except Exception as e:
                    results.append({"url": u, "success": False, "error": str(e)})
                completed += 1
                print(f"[batch-extract] Completed {completed}/{len(tasks)}")

        combined_out["processed"] = len(results)
        combined_out["articles"] = all_articles
        combined_out["total"] = len(all_articles)
        # Resolve output path robustly
        out_path = args.output or os.path.join(script_dir, "articles_combined.json")
        if not os.path.isabs(out_path):
            norm = out_path.replace("/", os.sep).replace("\\", os.sep)
            if not os.path.dirname(norm):
                out_path = os.path.join(script_dir, norm)
            else:
                out_path = os.path.join(project_root, norm)
        os.makedirs(os.path.dirname(out_path) or script_dir, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(combined_out, f, ensure_ascii=False, indent=2)
        print(json.dumps({"success": True, "mode": "combined", "output": out_path}, ensure_ascii=False))
        return

    # Combined with a specific target, or plain single selectors
    if is_combined:
        target = args.for_url or args.url
        if not target:
            raise SystemExit("When using combined selectors, provide --for-url to choose which site to extract, or omit to extract all with --concurrency.")
        tgt_domain = urlparse(target).netloc or target
        picked = None
        for e in sel_json["entries"]:
            d = (e.get("domain") or "").lower()
            if d and (d in tgt_domain.lower() or tgt_domain.lower() in d):
                picked = e
                break
        if not picked:
            raise SystemExit(f"No entry found in combined selectors for: {tgt_domain}")
        sections = picked.get("sections") or []
        # Also reset URL to picked url if provided
        if picked.get("url"):
            args.url = picked["url"]
    else:
        sections = sel_json.get("sections") or []
    out = extract_via_selectors(
        url=args.url,
        sections=sections,
        headful=args.headful,
        slowmo_ms=args.slowmo,
        max_items=args.max_items,
    )
    # Resolve output path robustly
    out_path = args.output
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    base_dir_name = os.path.basename(script_dir)
    if not os.path.isabs(out_path):
        norm = out_path.replace("/", os.sep).replace("\\", os.sep)
        # If path starts with 'python_tools/' while we're inside python_tools, drop the prefix
        if norm.lower().startswith(base_dir_name.lower() + os.sep):
            out_path = os.path.join(script_dir, os.path.basename(norm))
        else:
            # If no directory part, write into script_dir
            if not os.path.dirname(norm):
                out_path = os.path.join(script_dir, norm)
            else:
                # Otherwise, write relative to project root
                out_path = os.path.join(project_root, norm)
    # Ensure destination directory exists
    out_parent = os.path.dirname(out_path) or script_dir
    os.makedirs(out_parent, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(json.dumps({"success": True, "output": out_path}, ensure_ascii=False))


if __name__ == "__main__":
    _cli()



