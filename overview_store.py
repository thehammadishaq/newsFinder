import os
import csv
import time
import sqlite3
from typing import Dict, Any, List


# Default paths
BASE_DIR = os.path.dirname(__file__) or "."
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "pipelines_overview.sqlite3")
DEFAULT_CSV_PATH = os.path.join(BASE_DIR, "pipelines_overview.csv")


# CSV header (and DB columns) in exact order
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


def _connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=5.0, isolation_level=None)  # autocommit mode
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA foreign_keys=ON;")
    except Exception:
        pass
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    conn = _connect(db_path)
    try:
        cols = [f'"{h}" TEXT' for h in CSV_HEADER]
        # Primary key on Domain (sources)
        cols[0] = f'"{CSV_HEADER[0]}" TEXT PRIMARY KEY'
        sql = f"CREATE TABLE IF NOT EXISTS pipelines_overview ({', '.join(cols)})"
        conn.execute(sql)
        # Ensure all columns exist (auto-migrate older DBs)
        try:
            cur = conn.execute("PRAGMA table_info(pipelines_overview)")
            existing_cols = {row[1] for row in cur.fetchall()}  # row[1] is column name
            for h in CSV_HEADER:
                if h not in existing_cols:
                    try:
                        conn.execute(f'ALTER TABLE pipelines_overview ADD COLUMN "{h}" TEXT')
                    except Exception:
                        pass
        except Exception:
            pass
    finally:
        conn.close()


def upsert_overview(domain: str, updates: Dict[str, Any], db_path: str = DEFAULT_DB_PATH) -> None:
    # Retry on database lock
    for attempt in range(5):
        conn = _connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")  # reserve write lock
            # Fetch existing row (quoted identifiers without backslashes)
            select_cols = ", ".join(['"{}"'.format(h) for h in CSV_HEADER])
            pk = CSV_HEADER[0]
            cur = conn.execute(
                f'SELECT {select_cols} FROM pipelines_overview WHERE "{pk}" = ?',
                (domain,),
            )
            row = cur.fetchone()
            if row is None:
                current = _default_row(domain)
            else:
                current = {CSV_HEADER[i]: (row[i] if row[i] is not None else "") for i in range(len(CSV_HEADER))}

            # Apply updates with merge rules
            current[CSV_HEADER[0]] = domain
            for k, v in (updates or {}).items():
                if k not in CSV_HEADER or v is None:
                    continue
                if k == "Overall pipelines Error Details":
                    current[k] = _merge_overall_error(current.get(k) or "", str(v))
                elif k == "Overall pipelines Explanation":
                    current[k] = _merge_friendly_explanation(current.get(k) or "", str(v))
                else:
                    current[k] = str(v)

            # Ensure all columns exist
            for h in CSV_HEADER:
                current.setdefault(h, "")

            # Build UPSERT
            placeholders = ", ".join(["?"] * len(CSV_HEADER))
            colnames = ", ".join(['"{}"'.format(h) for h in CSV_HEADER])
            update_set = ", ".join(['"{0}" = excluded."{0}"'.format(h) for h in CSV_HEADER[1:]])
            values = [current[h] for h in CSV_HEADER]
            pk = CSV_HEADER[0]
            sql = (
                f'INSERT INTO pipelines_overview ({colnames}) VALUES ({placeholders}) '
                f'ON CONFLICT("{pk}") DO UPDATE SET {update_set}'
            )
            conn.execute(sql, values)
            conn.execute("COMMIT")
            return
        except sqlite3.OperationalError as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            # Database is locked -> small backoff and retry
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(0.05 * (2 ** attempt))
                continue
            else:
                return
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return
        finally:
            conn.close()


def export_csv(csv_path: str = DEFAULT_CSV_PATH, db_path: str = DEFAULT_DB_PATH) -> None:
    conn = _connect(db_path)
    tmp = csv_path + ".tmp"
    try:
        colnames = ", ".join(['"{}"'.format(h) for h in CSV_HEADER])
        pk = CSV_HEADER[0]
        cur = conn.execute(
            f'SELECT {colnames} FROM pipelines_overview ORDER BY "{pk}" ASC'
        )
        rows = cur.fetchall()
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_HEADER)
            w.writeheader()
            for r in rows:
                out = {CSV_HEADER[i]: (r[i] if r[i] is not None else "") for i in range(len(CSV_HEADER))}
                w.writerow(out)
        try:
            os.replace(tmp, csv_path)
        except Exception:
            try:
                if os.path.exists(csv_path):
                    os.remove(csv_path)
            except Exception:
                pass
            try:
                os.rename(tmp, csv_path)
            except Exception:
                pass
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
    finally:
        conn.close()


