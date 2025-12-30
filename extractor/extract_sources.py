from __future__ import annotations

import argparse
from pathlib import Path

from db import connect, init_sources_db, link_recipe_endpoint, upsert_endpoint, upsert_recipe
from recipe_parser import parse_recipe_file
from utils import DATA_DIR, RECIPES_DIR, classify_url, now_utc_iso, normalize_url, sha256_file


def build_recipe_uid(path: Path, recipes_dir: Path) -> str:
    rel = path.relative_to(recipes_dir)
    stem = rel.with_suffix("").as_posix()
    return f"builtin:{stem}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract recipe endpoints into sources.db")
    parser.add_argument("--db", default=str(DATA_DIR / "sources.db"), help="Path to sources.db")
    parser.add_argument("--recipes-dir", default=str(RECIPES_DIR), help="Path to recipes directory")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of recipes (for testing)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    recipes_dir = Path(args.recipes_dir)
    db_path = Path(args.db)

    conn = connect(db_path)
    init_sources_db(conn)

    count = 0
    for path in sorted(recipes_dir.glob("*.recipe")):
        if args.limit and count >= args.limit:
            break
        count += 1
        recipe_uid = build_recipe_uid(path, recipes_dir)
        meta, hits, error = parse_recipe_file(path)
        meta.recipe_uid = recipe_uid

        payload = {
            "recipe_uid": meta.recipe_uid,
            "title": meta.title or path.stem,
            "author": meta.author,
            "description": meta.description,
            "language": meta.language,
            "publication_type": meta.publication_type,
            "needs_subscription": meta.needs_subscription,
            "class_name": meta.class_name,
            "file_path": str(path),
            "file_sha256": sha256_file(path),
            "parse_status": "error" if error else "ok",
            "parse_error": error,
            "last_parsed": now_utc_iso(),
        }
        recipe_id = upsert_recipe(conn, payload)

        for hit in hits:
            url = normalize_url(hit.url)
            if not url:
                continue
            url_type = hit.url_type or classify_url(url, source=hit.source)
            endpoint_id = upsert_endpoint(conn, url, url_type)
            link_recipe_endpoint(
                conn,
                recipe_id=recipe_id,
                endpoint_id=endpoint_id,
                source=hit.source,
                context=hit.context,
                feed_title=hit.feed_title,
                raw_url=hit.raw_url,
                confidence=hit.confidence,
            )

        if args.verbose:
            status = "ERR" if error else "OK"
            print(f"[{status}] {recipe_uid} endpoints={len(hits)}")

    conn.commit()
    if args.verbose:
        print(f"Processed {count} recipes into {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
