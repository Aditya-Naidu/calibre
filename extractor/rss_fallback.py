from __future__ import annotations

import xml.etree.ElementTree as ET
from urllib.request import Request, urlopen


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_child_text(node: ET.Element, name: str) -> str | None:
    for child in node:
        if _strip_ns(child.tag) == name:
            if child.text:
                return child.text.strip()
    return None


def _find_child(node: ET.Element, name: str) -> ET.Element | None:
    for child in node:
        if _strip_ns(child.tag) == name:
            return child
    return None


def fetch_feed_xml(url: str, timeout: int = 30) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_feed(xml_bytes: bytes) -> tuple[str | None, list[dict]]:
    root = ET.fromstring(xml_bytes)
    root_tag = _strip_ns(root.tag)
    if root_tag == "rss":
        channel = _find_child(root, "channel")
        if channel is None:
            return None, []
        title = _find_child_text(channel, "title")
        entries = []
        for item in channel.findall("item"):
            entry = {
                "title": _find_child_text(item, "title"),
                "link": _find_child_text(item, "link"),
                "guid": _find_child_text(item, "guid"),
                "published": _find_child_text(item, "pubDate"),
                "summary": _find_child_text(item, "description"),
                "content": None,
            }
            # content:encoded
            for child in item:
                if _strip_ns(child.tag) == "encoded" and child.text:
                    entry["content"] = child.text
                    break
            entries.append(entry)
        return title, entries

    if root_tag == "feed":
        title = _find_child_text(root, "title")
        entries = []
        for item in root.findall("entry"):
            link = None
            for link_el in item.findall("link"):
                rel = link_el.attrib.get("rel", "alternate")
                if rel == "alternate":
                    link = link_el.attrib.get("href")
                    break
                if link is None:
                    link = link_el.attrib.get("href")
            entry = {
                "title": _find_child_text(item, "title"),
                "link": link,
                "guid": _find_child_text(item, "id"),
                "published": _find_child_text(item, "published") or _find_child_text(item, "updated"),
                "summary": _find_child_text(item, "summary"),
                "content": _find_child_text(item, "content"),
            }
            entries.append(entry)
        return title, entries

    return None, []
