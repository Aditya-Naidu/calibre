from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

from db import RecipeRow, connect, init_news_db, record_run
from rss_fallback import fetch_feed_xml, parse_feed
from utils import DATA_DIR, REPO_ROOT, html_to_text, now_utc_iso


@dataclass
class FetchOptions:
    max_articles: int
    verbose: bool
    keep_temp: bool


def _hash_text(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8", "replace"))
    return h.hexdigest()


def _fingerprint(*parts: str | None) -> str:
    joined = "|".join([p or "" for p in parts])
    return _hash_text(joined)


def _insert_article(conn, payload: dict) -> None:
    keys = sorted(payload.keys())
    cols = ",".join(keys)
    placeholders = ",".join([":" + k for k in keys])
    conn.execute(
        f"INSERT OR IGNORE INTO articles ({cols}) VALUES ({placeholders})",
        payload,
    )


def _load_recipes(conn) -> list[RecipeRow]:
    rows = conn.execute(
        "SELECT id, recipe_uid, title, file_path FROM recipes ORDER BY recipe_uid"
    ).fetchall()
    return [RecipeRow(id=row["id"], recipe_uid=row["recipe_uid"], title=row["title"], file_path=row["file_path"]) for row in rows]


def _load_feed_endpoints(conn) -> dict[str, list[dict]]:
    rows = conn.execute(
        """
        SELECT r.recipe_uid, r.title as recipe_title, e.url, re.feed_title
        FROM recipe_endpoints re
        JOIN endpoints e ON e.id = re.endpoint_id
        JOIN recipes r ON r.id = re.recipe_id
        WHERE e.url_type = 'feed'
        ORDER BY r.recipe_uid
        """
    ).fetchall()
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["recipe_uid"], []).append(dict(row))
    return grouped


class SimpleBrowser:
    def __init__(self, user_agent: str) -> None:
        from http.cookiejar import CookieJar
        from urllib.request import build_opener, HTTPCookieProcessor

        self.cookiejar = CookieJar()
        self.addheaders = [("User-agent", user_agent), ("Accept", "*/*")]
        self._opener = build_opener(HTTPCookieProcessor(self.cookiejar))

    def _headers(self) -> dict[str, str]:
        return {k: v for k, v in self.addheaders if v is not None}

    def open(self, url, data=None, timeout=30):
        from urllib.parse import urlencode
        from urllib.request import Request

        if isinstance(data, dict):
            data = urlencode(data).encode("utf-8")
        elif isinstance(data, str):
            data = data.encode("utf-8")
        req = Request(url, data=data, headers=self._headers())
        return self._opener.open(req, timeout=timeout)

    def open_novisit(self, url, data=None, timeout=30):
        return self.open(url, data=data, timeout=timeout)

    def set_handle_gzip(self, *_args, **_kwargs):
        return None

    def set_cookie(self, *_args, **_kwargs):
        return None

    def clone_browser(self):
        clone = SimpleBrowser(self._headers().get("User-agent", "Mozilla/5.0"))
        clone.cookiejar = self.cookiejar
        clone.addheaders = list(self.addheaders)
        return clone


def _install_simple_browser() -> None:
    from calibre.web.feeds import news as news_mod

    def simple_get_browser(self, *args, **kwargs):
        ua = getattr(self, "last_used_user_agent", None)
        if not ua:
            try:
                from calibre import random_user_agent

                ua = random_user_agent(allow_ie=False)
            except Exception:
                ua = "Mozilla/5.0"
        self.last_used_user_agent = ua
        return SimpleBrowser(ua)

    simple_get_browser.is_base_class_implementation = True
    news_mod.BasicNewsRecipe.get_browser = simple_get_browser


def _recipe_mode_available() -> bool:
    try:
        sys.path.insert(0, str(REPO_ROOT / "src"))
        import calibre  # noqa: F401
        return True
    except Exception:
        return False


def _run_recipe_mode(
    sources_conn,
    news_conn,
    options: FetchOptions,
    recipe_filter: set[str] | None,
) -> None:
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from calibre.utils.logging import Log
    from calibre.web.feeds.recipes import compile_recipe

    try:
        import mechanize  # noqa: F401
        has_mech = True
    except Exception:
        has_mech = False

    if not has_mech:
        _install_simple_browser()

    recipes = _load_recipes(sources_conn)
    for recipe in recipes:
        if recipe_filter and recipe.recipe_uid not in recipe_filter:
            continue
        started_at = now_utc_iso()
        error = None
        article_count = 0
        status = "ok"

        if options.verbose:
            print(f"[recipe] {recipe.recipe_uid} -> {recipe.file_path}")

        try:
            src = Path(recipe.file_path).read_text(encoding="utf-8", errors="replace")
            recipe_class = compile_recipe(src)
            if recipe_class is None:
                raise RuntimeError("No recipe class found")

            class Options:
                verbose = 2 if options.verbose else 0
                test = None
                username = None
                password = None
                lrf = False
                recipe_specific_option = None

                class OutputProfile:
                    short_name = "default"
                    touchscreen = False
                    touchscreen_news_css = ""
                    screen_size = (600, 800)
                    periodical_date_in_title = False

                output_profile = OutputProfile()

            log = Log(level=Log.DEBUG if options.verbose else Log.INFO)

            def progress(_pct, _msg=None):
                return None

            with TemporaryDirectory() as tmp:
                old_cwd = os.getcwd()
                os.chdir(tmp)
                try:
                    recipe_obj = recipe_class(Options, log, progress)
                    recipe_obj.download()
                    for feed_index, feed in enumerate(recipe_obj.feed_objects or []):
                        for article_index, article in enumerate(feed):
                            if article_index >= options.max_articles:
                                break
                            html_src = ""
                            if getattr(article, "downloaded", False):
                                file_path = Path(tmp) / f"feed_{feed_index}" / f"article_{article_index}" / "index.html"
                                if file_path.exists():
                                    html_src = file_path.read_text(encoding="utf-8", errors="replace")
                            if not html_src and getattr(article, "content", None):
                                html_src = article.content
                            if not html_src and getattr(article, "summary", None):
                                html_src = article.summary

                            content_text = html_to_text(html_src) if html_src else ""
                            content_hash = _hash_text(html_src) if html_src else None

                            published = None
                            if getattr(article, "localtime", None):
                                published = article.localtime.isoformat()

                            article_url = getattr(article, "orig_url", None) or getattr(article, "url", None)
                            fingerprint = _fingerprint(recipe.recipe_uid, article_url, article.title, published)

                            _insert_article(
                                news_conn,
                                {
                                    "recipe_uid": recipe.recipe_uid,
                                    "recipe_title": recipe.title,
                                    "feed_title": getattr(feed, "title", None),
                                    "article_title": article.title,
                                    "article_url": article_url,
                                    "article_guid": getattr(article, "id", None),
                                    "author": getattr(article, "author", None),
                                    "summary": getattr(article, "summary", None),
                                    "published": published,
                                    "fetched_at": now_utc_iso(),
                                    "content_html": html_src,
                                    "content_text": content_text,
                                    "content_hash": content_hash,
                                    "fingerprint": fingerprint,
                                },
                            )
                            article_count += 1
                finally:
                    os.chdir(old_cwd)
                if options.keep_temp:
                    print(f"Preserved temp dir: {tmp}")
        except Exception as exc:
            status = "error"
            error = f"{type(exc).__name__}: {exc}"
            if options.verbose:
                print(f"[recipe] failed {recipe.recipe_uid}: {error}")

        finished_at = now_utc_iso()
        record_run(news_conn, recipe.recipe_uid, status, started_at, finished_at, error, article_count)
        news_conn.commit()


def _run_rss_mode(
    sources_conn,
    news_conn,
    options: FetchOptions,
    recipe_filter: set[str] | None,
) -> None:
    feed_map = _load_feed_endpoints(sources_conn)

    for recipe_uid, feeds in feed_map.items():
        if recipe_filter and recipe_uid not in recipe_filter:
            continue
        started_at = now_utc_iso()
        error = None
        status = "ok"
        article_count = 0

        for feed in feeds:
            url = feed["url"]
            try:
                xml_bytes = fetch_feed_xml(url)
                feed_title, entries = parse_feed(xml_bytes)
                if feed_title is None:
                    feed_title = feed.get("feed_title")
                for idx, entry in enumerate(entries):
                    if idx >= options.max_articles:
                        break
                    html_src = entry.get("content") or entry.get("summary") or ""
                    content_text = html_to_text(html_src) if html_src else ""
                    content_hash = _hash_text(html_src) if html_src else None
                    published = entry.get("published")
                    article_url = entry.get("link")
                    fingerprint = _fingerprint(recipe_uid, article_url, entry.get("title"), published)
                    _insert_article(
                        news_conn,
                        {
                            "recipe_uid": recipe_uid,
                            "recipe_title": feed.get("recipe_title"),
                            "feed_title": feed_title,
                            "article_title": entry.get("title"),
                            "article_url": article_url,
                            "article_guid": entry.get("guid"),
                            "author": None,
                            "summary": entry.get("summary"),
                            "published": published,
                            "fetched_at": now_utc_iso(),
                            "content_html": html_src,
                            "content_text": content_text,
                            "content_hash": content_hash,
                            "fingerprint": fingerprint,
                        },
                    )
                    article_count += 1
            except Exception as exc:
                status = "error"
                error = f"{type(exc).__name__}: {exc}"
                if options.verbose:
                    print(f"[rss] failed {recipe_uid} feed={url}: {error}")

        finished_at = now_utc_iso()
        record_run(news_conn, recipe_uid, status, started_at, finished_at, error, article_count)
        news_conn.commit()


def _parse_recipe_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    return {v.strip() for v in value.split(",") if v.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch news into news.db")
    parser.add_argument("--sources-db", default=str(DATA_DIR / "sources.db"))
    parser.add_argument("--news-db", default=str(DATA_DIR / "news.db"))
    parser.add_argument("--mode", choices=["auto", "recipe", "rss"], default="auto")
    parser.add_argument("--recipe", default=None, help="Comma-separated recipe_uids to run")
    parser.add_argument("--max-articles", type=int, default=100)
    parser.add_argument("--interval", type=int, default=0, help="Seconds between runs (0 for one-shot)")
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    sources_conn = connect(Path(args.sources_db))
    news_conn = connect(Path(args.news_db))
    init_news_db(news_conn)

    recipe_filter = _parse_recipe_filter(args.recipe)
    options = FetchOptions(max_articles=args.max_articles, verbose=args.verbose, keep_temp=args.keep_temp)

    def run_once() -> None:
        if args.mode == "recipe":
            _run_recipe_mode(sources_conn, news_conn, options, recipe_filter)
            return
        if args.mode == "rss":
            _run_rss_mode(sources_conn, news_conn, options, recipe_filter)
            return
        if _recipe_mode_available():
            try:
                _run_recipe_mode(sources_conn, news_conn, options, recipe_filter)
                return
            except Exception as exc:
                if args.verbose:
                    print(f"[auto] recipe mode failed: {exc}; falling back to rss")
        _run_rss_mode(sources_conn, news_conn, options, recipe_filter)

    if args.interval > 0:
        while True:
            run_once()
            time.sleep(args.interval)
    else:
        run_once()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
