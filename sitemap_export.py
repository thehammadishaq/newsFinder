import os
import json
from typing import List, Dict, Any
from sitemap_discovery import gather_all_entries_from_robots


def _cli():
    import argparse
    parser = argparse.ArgumentParser(description="Export url+date entries from a site's sitemaps via robots.txt (no heuristics)")
    parser.add_argument("url", help="Root URL (homepage or any page of the site)")
    parser.add_argument("--output", dest="output", default="sitemap_entries.json", help="Output JSON path")
    args = parser.parse_args()

    entries: List[Dict[str, Any]] = gather_all_entries_from_robots(args.url)
    out = {"success": True, "total": len(entries), "entries": entries}

    # Resolve output path robustly
    out_path = args.output
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    base_dir_name = os.path.basename(script_dir)
    if not os.path.isabs(out_path):
        norm = out_path.replace("/", os.sep).replace("\\", os.sep)
        if not os.path.dirname(norm):
            out_path = os.path.join(script_dir, norm)
        else:
            if norm.lower().startswith(base_dir_name.lower() + os.sep):
                # If user ran inside python_tools and prefixed path with python_tools/..., write under script_dir
                out_path = os.path.join(script_dir, os.path.basename(norm))
            else:
                # Otherwise, treat as relative to project root
                out_path = os.path.join(project_root, norm)

    out_parent = os.path.dirname(out_path) or script_dir
    os.makedirs(out_parent, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(json.dumps({"success": True, "output": out_path, "total": len(entries)}, ensure_ascii=False))


if __name__ == "__main__":
    _cli()


