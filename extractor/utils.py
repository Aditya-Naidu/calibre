from __future__ import annotations

import hashlib
import html
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
RECIPES_DIR = REPO_ROOT / "recipes"
DATA_DIR = Path(__file__).resolve().parent / "data"


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def html_to_text(html_src: str) -> str:
    parser = _HTMLTextExtractor()
    parser.feed(html_src)
    parser.close()
    text = parser.get_text()
    return " ".join(text.split())


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def iter_recipe_files(recipes_dir: Path = RECIPES_DIR) -> Iterable[Path]:
    for path in sorted(recipes_dir.glob("*.recipe")):
        if path.is_file():
            yield path


def normalize_url(raw: str) -> str | None:
    raw = raw.strip().strip("'\"")
    if not raw:
        return None
    # Trim trailing punctuation commonly attached in source strings.
    raw = raw.rstrip(")]>.,;\"' ")
    if raw.startswith("feed://"):
        raw = "http://" + raw[len("feed://"):]
    return raw


def classify_url(url: str, source: str | None = None, attr_name: str | None = None) -> str:
    u = url.lower()
    if source == "feeds" or (attr_name and "feed" in attr_name.lower()):
        return "feed"
    if any(x in u for x in ("/rss", ".rss", "/atom", ".atom", ".xml")):
        return "feed"
    if "/api/" in u or ".json" in u or "api." in u:
        return "api"
    return "html"


@dataclass
class EndpointHit:
    url: str
    url_type: str
    source: str
    context: str | None = None
    feed_title: str | None = None
    raw_url: str | None = None
    confidence: float = 0.5


def find_urls_in_text(text: str) -> list[str]:
    pattern = re.compile(r"(https?://[^\s'\"<>]+|feed://[^\s'\"<>]+)")
    return pattern.findall(text)


def decode_html_entities(text: str) -> str:
    return html.unescape(text)
