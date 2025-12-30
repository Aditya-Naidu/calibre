from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from utils import ensure_dir, now_utc_iso


SOURCES_SCHEMA = """
CREATE TABLE IF NOT EXISTS recipes (
    id INTEGER PRIMARY KEY,
    recipe_uid TEXT UNIQUE,
    title TEXT,
    author TEXT,
    description TEXT,
    language TEXT,
    publication_type TEXT,
    needs_subscription TEXT,
    class_name TEXT,
    file_path TEXT,
    file_sha256 TEXT,
    parse_status TEXT,
    parse_error TEXT,
    last_parsed TEXT
);

CREATE TABLE IF NOT EXISTS endpoints (
    id INTEGER PRIMARY KEY,
    url TEXT UNIQUE,
    url_type TEXT,
    scheme TEXT,
    domain TEXT,
    path TEXT,
    query TEXT
);

CREATE TABLE IF NOT EXISTS recipe_endpoints (
    recipe_id INTEGER NOT NULL,
    endpoint_id INTEGER NOT NULL,
    context TEXT,
    feed_title TEXT,
    source TEXT,
    confidence REAL,
    raw_url TEXT,
    first_seen TEXT,
    last_seen TEXT,
    UNIQUE(recipe_id, endpoint_id, context, feed_title, source)
);
"""


NEWS_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY,
    recipe_uid TEXT,
    started_at TEXT,
    finished_at TEXT,
    status TEXT,
    error TEXT,
    article_count INTEGER
);

CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY,
    recipe_uid TEXT NOT NULL,
    recipe_title TEXT,
    feed_title TEXT,
    article_title TEXT,
    article_url TEXT,
    article_guid TEXT,
    author TEXT,
    summary TEXT,
    published TEXT,
    fetched_at TEXT,
    content_html TEXT,
    content_text TEXT,
    content_hash TEXT,
    fingerprint TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_articles_recipe_uid ON articles(recipe_uid);
"""


@dataclass
class RecipeRow:
    id: int
    recipe_uid: str
    title: str | None
    file_path: str


def connect(db_path: Path) -> sqlite3.Connection:
    ensure_dir(db_path.parent)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_sources_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SOURCES_SCHEMA)
    conn.commit()


def init_news_db(conn: sqlite3.Connection) -> None:
    conn.executescript(NEWS_SCHEMA)
    conn.commit()


def upsert_recipe(conn: sqlite3.Connection, payload: dict) -> int:
    keys = sorted(payload.keys())
    cols = ",".join(keys)
    placeholders = ",".join([":" + k for k in keys])
    updates = ",".join([f"{k}=excluded.{k}" for k in keys if k != "recipe_uid"])
    sql = f"""
        INSERT INTO recipes ({cols})
        VALUES ({placeholders})
        ON CONFLICT(recipe_uid) DO UPDATE SET {updates}
    """
    conn.execute(sql, payload)
    row = conn.execute("SELECT id FROM recipes WHERE recipe_uid = ?", (payload["recipe_uid"],)).fetchone()
    return int(row[0])


def upsert_endpoint(conn: sqlite3.Connection, url: str, url_type: str) -> int:
    parts = urlsplit(url)
    payload = {
        "url": url,
        "url_type": url_type,
        "scheme": parts.scheme,
        "domain": parts.netloc,
        "path": parts.path,
        "query": parts.query,
    }
    conn.execute(
        """
        INSERT INTO endpoints (url, url_type, scheme, domain, path, query)
        VALUES (:url, :url_type, :scheme, :domain, :path, :query)
        ON CONFLICT(url) DO UPDATE SET url_type=excluded.url_type
        """,
        payload,
    )
    row = conn.execute("SELECT id FROM endpoints WHERE url = ?", (url,)).fetchone()
    return int(row[0])


def link_recipe_endpoint(
    conn: sqlite3.Connection,
    recipe_id: int,
    endpoint_id: int,
    source: str,
    context: str | None,
    feed_title: str | None,
    raw_url: str | None,
    confidence: float,
) -> None:
    ts = now_utc_iso()
    conn.execute(
        """
        INSERT INTO recipe_endpoints (
            recipe_id, endpoint_id, context, feed_title, source,
            confidence, raw_url, first_seen, last_seen
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recipe_id, endpoint_id, context, feed_title, source)
        DO UPDATE SET last_seen=excluded.last_seen
        """,
        (recipe_id, endpoint_id, context, feed_title, source, confidence, raw_url, ts, ts),
    )


def record_run(
    conn: sqlite3.Connection,
    recipe_uid: str,
    status: str,
    started_at: str,
    finished_at: str,
    error: str | None,
    article_count: int,
) -> None:
    conn.execute(
        """
        INSERT INTO runs (recipe_uid, started_at, finished_at, status, error, article_count)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (recipe_uid, started_at, finished_at, status, error, article_count),
    )
