import os
import sys
import json
import argparse
from glob import glob
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone


def _parse_ts(ts_str: Optional[str]) -> float:
    if not ts_str:
        return 0.0
    s = str(ts_str).strip()
    # Expected like: 2025-10-30 15:02:57 UTC
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S %Z")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        pass
    # Fallback: try ISO
    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
            return datetime.fromisoformat(s2).timestamp()
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def _signature_for_section(section: Dict[str, Any]) -> str:
    sel = section.get("selectors") or {}
    title = str(sel.get("title", "") or "").strip()
    link = str(sel.get("link", "") or "").strip()
    return f"{title}|{link}"


def _collect_inputs(streams: List[str], streams_glob: Optional[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for p in streams or []:
        if not p:
            continue
        ap = os.path.abspath(p)
        if os.path.exists(ap) and ap not in seen:
            seen.add(ap)
            out.append(ap)
    if streams_glob:
        for p in glob(streams_glob):
            ap = os.path.abspath(p)
            if os.path.exists(ap) and ap not in seen:
                seen.add(ap)
                out.append(ap)
    return out


def _iter_jsonl(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = (line or '').strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _aggregate(stream_paths: List[str], strict: bool = True) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Any]]]:
    # site_latest[site] = { 'result': {...}, 'ts': float }
    site_latest: Dict[str, Dict[str, Any]] = {}
    # sitemap_by_leaf[leaf_url] = { 'source': site, 'det': detectedSelectors, 'fields_len': int, 'ts': float }
    sitemap_by_leaf: Dict[str, Dict[str, Any]] = {}
    # css_by_key[(pageUrl, sig)] = { 'source': site, 'pageUrl': str, 'section': {...}, 'conf': float, 'ts': float }
    css_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for path in stream_paths:
        for row in _iter_jsonl(path):
            ts = _parse_ts(row.get('timestamp'))
            result = row.get('result') if isinstance(row.get('result'), dict) else None
            if not isinstance(result, dict):
                if strict:
                    continue
                else:
                    result = {}
            site = str((result.get('url') or '')).strip()
            if not site:
                if strict:
                    continue
                else:
                    site = ''

            # Track latest site aggregate by timestamp
            prev = site_latest.get(site)
            if prev is None or float(ts) >= float(prev.get('ts') or 0.0):
                site_latest[site] = {'result': result, 'ts': float(ts)}

            # Collect sitemap selectors
            llm = result.get('llmDetection') or {}
            sel_list = llm.get('selectors') or []
            if isinstance(sel_list, list):
                for it in sel_list:
                    if not isinstance(it, dict):
                        continue
                    leaf_url = str(it.get('url') or '').strip()
                    det = it.get('detectedSelectors') or {}
                    fields = det.get('fields') if isinstance(det, dict) else None
                    if not leaf_url or not isinstance(fields, dict) or not fields:
                        continue
                    fields_len = len(fields)
                    prev_sm = sitemap_by_leaf.get(leaf_url)
                    if (prev_sm is None) or (fields_len > int(prev_sm.get('fields_len') or 0)) or (fields_len == int(prev_sm.get('fields_len') or 0) and float(ts) >= float(prev_sm.get('ts') or 0.0)):
                        sitemap_by_leaf[leaf_url] = {
                            'source': site,
                            'det': det,
                            'fields_len': fields_len,
                            'ts': float(ts),
                        }

            # Collect CSS selectors
            cssf = result.get('cssFallback') or {}
            if bool(cssf.get('triggered')) and bool(cssf.get('success')):
                csssel = cssf.get('selectors') or {}
                # Prefer per-section sourceUrl if present; fallback to top-level pageUrl -> site
                page_url_default = str(csssel.get('pageUrl') or site or '').strip()
                sections = csssel.get('sections') or []
                # If no top-level sections but perPage present, flatten it
                if (not sections) and isinstance(csssel.get('perPage'), dict):
                    flat_sections: List[Dict[str, Any]] = []
                    try:
                        for purl, arr in (csssel.get('perPage') or {}).items():
                            if not isinstance(arr, list):
                                continue
                            for sec in arr:
                                if not isinstance(sec, dict):
                                    continue
                                if not sec.get('sourceUrl'):
                                    tmp = dict(sec)
                                    tmp['sourceUrl'] = str(purl or page_url_default)
                                    flat_sections.append(tmp)
                                else:
                                    flat_sections.append(sec)
                    except Exception:
                        flat_sections = []
                    sections = flat_sections
                if isinstance(sections, list):
                    for section in sections:
                        if not isinstance(section, dict):
                            continue
                        sig = _signature_for_section(section)
                        if not sig or sig == '|':
                            continue
                        # Resolve pageUrl per section
                        page_url = str(section.get('sourceUrl') or page_url_default or '').strip()
                        if not page_url:
                            continue
                        # Safe confidence parsing
                        conf_val = section.get('confidence')
                        try:
                            if conf_val is None:
                                conf = 0.0
                            elif isinstance(conf_val, (int, float)):
                                conf = float(conf_val)
                            else:
                                s = str(conf_val).strip().lower()
                                if s.endswith('%'):
                                    num = float(s[:-1])
                                    conf = max(0.0, min(1.0, num / 100.0))
                                elif s in ('very high','vhigh','v-high'):
                                    conf = 0.98
                                elif s == 'high':
                                    conf = 0.9
                                elif s in ('medium','med'):
                                    conf = 0.6
                                elif s == 'low':
                                    conf = 0.3
                                else:
                                    v = float(s)
                                    conf = v if 0.0 <= v <= 1.0 else max(0.0, min(1.0, v / 100.0))
                        except Exception:
                            conf = 0.0
                        key = (page_url, sig)
                        prev_css = css_by_key.get(key)
                        if (prev_css is None) or (conf > float(prev_css.get('conf') or 0.0)) or (conf == float(prev_css.get('conf') or 0.0) and float(ts) >= float(prev_css.get('ts') or 0.0)):
                            css_by_key[key] = {
                                'source': site,
                                'pageUrl': page_url,
                                'section': section,
                                'conf': conf,
                                'ts': float(ts),
                            }

    return site_latest, sitemap_by_leaf, css_by_key


def _write_outputs(site_latest: Dict[str, Dict[str, Any]],
                   sitemap_by_leaf: Dict[str, Dict[str, Any]],
                   css_by_key: Dict[Tuple[str, str], Dict[str, Any]],
                   out_dir: str,
                   ts_suffix: bool = True) -> Tuple[str, str, str]:
    os.makedirs(out_dir or '.', exist_ok=True)
    now = datetime.utcnow()
    suf_full = now.strftime('%Y%m%d_%H%M%SZ') if ts_suffix else 'latest'
    suf_day = now.strftime('%Y%m%d') if ts_suffix else 'latest'

    out_overall = os.path.join(out_dir, f'selection_aggregate_{suf_full}.jsonl')
    out_sitemap = os.path.join(out_dir, f'selection_targets_sitemap_{suf_day}.json')
    out_css = os.path.join(out_dir, f'selection_targets_css_{suf_day}.json')

    # 1) Overall JSONL (most recent per site)
    with open(out_overall, 'w', encoding='utf-8') as f:
        # optional: include counts of dedup’d items per site
        # Build quick maps for counts
        sm_by_site: Dict[str, int] = {}
        for leaf, rec in sitemap_by_leaf.items():
            src = rec.get('source') or ''
            if src:
                sm_by_site[src] = sm_by_site.get(src, 0) + 1
        css_by_site: Dict[str, int] = {}
        for (page_url, sig), rec in css_by_key.items():
            src = rec.get('source') or ''
            if src:
                css_by_site[src] = css_by_site.get(src, 0) + 1

        for site, rec in sorted(site_latest.items(), key=lambda kv: kv[0]):
            result = rec.get('result') or {}
            # attach a small summary
            try:
                result = dict(result)
                result['mergedSelectorsSummary'] = {
                    'sitemapLeaves': int(sm_by_site.get(site, 0)),
                    'cssSections': int(css_by_site.get(site, 0)),
                }
            except Exception:
                pass
            obj = {
                'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
                'result': result,
            }
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')

    # 2) Sitemap targets JSON (group by source)
    by_source_sm: Dict[str, List[Dict[str, Any]]] = {}
    for leaf_url, rec in sitemap_by_leaf.items():
        src = str(rec.get('source') or '').strip()
        det = rec.get('det') or {}
        if not src or not isinstance(det, dict):
            continue
        by_source_sm.setdefault(src, []).append({'url': leaf_url, 'selectors': det})
    # Sort for stability
    sitemap_targets: List[Dict[str, Any]] = []
    for src in sorted(by_source_sm.keys()):
        leafs = sorted(by_source_sm[src], key=lambda it: it.get('url') or '')
        sitemap_targets.append({
            'source': src,
            'sourceType': 'sitemap',
            'leafSitemaps': leafs,
        })
    with open(out_sitemap, 'w', encoding='utf-8') as f:
        json.dump(sitemap_targets, f, ensure_ascii=False, indent=2)

    # 3) CSS targets JSON (group by source + page)
    by_src_page_css: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for (page_url, sig), rec in css_by_key.items():
        src = str(rec.get('source') or '').strip()
        if not src or not page_url:
            continue
        key = (src, page_url)
        entry = by_src_page_css.get(key) or {'source': src, 'sourceType': 'css', 'pageUrl': page_url, 'sections': []}
        entry['sections'].append(rec.get('section') or {})
        by_src_page_css[key] = entry
    css_targets: List[Dict[str, Any]] = []
    for (src, page_url), entry in sorted(by_src_page_css.items(), key=lambda it: (it[0][0], it[0][1])):
        # Stable sort sections by signature
        sections = entry.get('sections') or []
        try:
            sections = sorted(sections, key=_signature_for_section)
        except Exception:
            pass
        css_targets.append({
            'source': src,
            'sourceType': 'css',
            'pageUrl': page_url,
            'sections': sections,
        })
    with open(out_css, 'w', encoding='utf-8') as f:
        json.dump(css_targets, f, ensure_ascii=False, indent=2)

    return out_overall, out_sitemap, out_css


def _cli():
    p = argparse.ArgumentParser(description='Aggregate selection_extraction_report_stream*.jsonl into deduplicated artifacts')
    p.add_argument('--streams', action='append', default=[], help='Path to a stream JSONL file (can be repeated)')
    p.add_argument('--streams-glob', default=None, help='Glob pattern to include many JSONL files')
    p.add_argument('--out-dir', default='.', help='Output directory')
    p.add_argument('--no-ts-suffix', action='store_true', help='Do not add timestamp suffix to output filenames')
    p.add_argument('--strict', action='store_true', default=True, help='Skip invalid rows (default true)')
    args = p.parse_args()

    inputs = _collect_inputs(args.streams or [], args.streams_glob)
    if not inputs:
        print('[aggregate] No input streams found')
        sys.exit(1)
    print(f"[aggregate] Inputs: {len(inputs)} file(s)")
    for i, pth in enumerate(inputs, 1):
        print(f"  [{i}] {pth}")

    site_latest, sitemap_by_leaf, css_by_key = _aggregate(inputs, strict=bool(args.strict))
    print(f"[aggregate] Sites: {len(site_latest)} | Sitemap leaves unique: {len(sitemap_by_leaf)} | CSS sections unique: {len(css_by_key)}")

    out_overall, out_sitemap, out_css = _write_outputs(site_latest, sitemap_by_leaf, css_by_key, args.out_dir, ts_suffix=not bool(args.no_ts_suffix))
    print(f"[aggregate] Wrote overall JSONL -> {out_overall}")
    print(f"[aggregate] Wrote sitemap targets JSON -> {out_sitemap}")
    print(f"[aggregate] Wrote CSS targets JSON -> {out_css}")


if __name__ == '__main__':
    _cli()


