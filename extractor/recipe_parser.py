from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utils import EndpointHit, classify_url, decode_html_entities, find_urls_in_text, normalize_url


@dataclass
class RecipeMetadata:
    recipe_uid: str
    title: str | None
    author: str | None
    description: str | None
    language: str | None
    publication_type: str | None
    needs_subscription: str | None
    class_name: str | None


def _is_recipe_base(base: ast.expr) -> bool:
    if isinstance(base, ast.Name):
        return base.id in {
            "BasicNewsRecipe",
            "AutomaticNewsRecipe",
            "CustomIndexRecipe",
            "CalibrePeriodical",
        }
    if isinstance(base, ast.Attribute):
        return base.attr in {
            "BasicNewsRecipe",
            "AutomaticNewsRecipe",
            "CustomIndexRecipe",
            "CalibrePeriodical",
        }
    return False


def _eval_node(node: ast.AST, constants: dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Str):
        return node.s
    if isinstance(node, ast.Bytes):
        return node.s
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.Tuple):
        items = [_eval_node(elt, constants) for elt in node.elts]
        if any(item is None for item in items):
            return None
        return tuple(items)
    if isinstance(node, ast.List):
        items = [_eval_node(elt, constants) for elt in node.elts]
        if any(item is None for item in items):
            return None
        return list(items)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _eval_node(node.left, constants)
        right = _eval_node(node.right, constants)
        if isinstance(left, (str, bytes)) and isinstance(right, (str, bytes)):
            return left + right
        if isinstance(left, list) and isinstance(right, list):
            return left + right
    if isinstance(node, ast.JoinedStr):
        parts = []
        for value in node.values:
            val = _eval_node(value, constants)
            if val is None:
                return None
            parts.append(str(val))
        return "".join(parts)
    if isinstance(node, ast.FormattedValue):
        return _eval_node(node.value, constants)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == "format":
            base = _eval_node(node.func.value, constants)
            if not isinstance(base, str):
                return None
            args = [_eval_node(a, constants) for a in node.args]
            kwargs = {kw.arg: _eval_node(kw.value, constants) for kw in node.keywords if kw.arg}
            if any(a is None for a in args) or any(v is None for v in kwargs.values()):
                return None
            try:
                return base.format(*args, **kwargs)
            except Exception:
                return None
    return None


def _collect_module_constants(tree: ast.Module) -> dict[str, Any]:
    constants: dict[str, Any] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1:
                continue
            target = node.targets[0]
            if not isinstance(target, ast.Name):
                continue
            val = _eval_node(node.value, constants)
            if isinstance(val, (str, bytes, int, float, list, tuple)):
                constants[target.id] = val
    return constants


def _find_recipe_class(tree: ast.Module) -> ast.ClassDef | None:
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if any(_is_recipe_base(base) for base in node.bases):
                return node
    return None


def _extract_class_assignments(node: ast.ClassDef, constants: dict[str, Any]) -> dict[str, Any]:
    assigns: dict[str, Any] = {}
    for item in node.body:
        if isinstance(item, ast.Assign) and len(item.targets) == 1:
            target = item.targets[0]
            if isinstance(target, ast.Name):
                val = _eval_node(item.value, constants | assigns)
                if val is not None:
                    assigns[target.id] = val
        elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
            val = _eval_node(item.value, constants | assigns) if item.value else None
            if val is not None:
                assigns[item.target.id] = val
        elif isinstance(item, ast.AugAssign) and isinstance(item.target, ast.Name):
            if isinstance(item.op, ast.Add):
                current = assigns.get(item.target.id)
                extra = _eval_node(item.value, constants | assigns)
                if isinstance(current, list) and isinstance(extra, list):
                    assigns[item.target.id] = current + extra
    return assigns


def _feeds_to_endpoints(feeds: Any) -> list[EndpointHit]:
    hits: list[EndpointHit] = []
    if not isinstance(feeds, (list, tuple)):
        return hits
    for entry in feeds:
        if isinstance(entry, (str, bytes)):
            url = normalize_url(entry.decode("utf-8", "replace") if isinstance(entry, bytes) else entry)
            if url:
                hits.append(EndpointHit(url=url, url_type=classify_url(url, source="feeds"), source="feeds", raw_url=entry, confidence=0.95))
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            title, url = entry[0], entry[1]
            if isinstance(url, bytes):
                url = url.decode("utf-8", "replace")
            if isinstance(title, bytes):
                title = title.decode("utf-8", "replace")
            url_n = normalize_url(url) if isinstance(url, str) else None
            if url_n:
                hits.append(
                    EndpointHit(
                        url=url_n,
                        url_type=classify_url(url_n, source="feeds"),
                        source="feeds",
                        feed_title=str(title) if title is not None else None,
                        raw_url=str(url),
                        confidence=0.98,
                    )
                )
    return hits


def _attr_url_hits(assigns: dict[str, Any]) -> list[EndpointHit]:
    hits: list[EndpointHit] = []
    for name, value in assigns.items():
        if "url" in name.lower() and isinstance(value, (str, bytes)):
            url = value.decode("utf-8", "replace") if isinstance(value, bytes) else value
            url = normalize_url(url)
            if url:
                hits.append(
                    EndpointHit(
                        url=url,
                        url_type=classify_url(url, source="attr", attr_name=name),
                        source="attr",
                        context=name,
                        raw_url=str(value),
                        confidence=0.8,
                    )
                )
    return hits


def parse_recipe_file(path: Path) -> tuple[RecipeMetadata, list[EndpointHit], str | None]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    raw = decode_html_entities(raw)
    url_hits: list[EndpointHit] = []

    for found in find_urls_in_text(raw):
        url = normalize_url(found)
        if url:
            url_hits.append(
                EndpointHit(
                    url=url,
                    url_type=classify_url(url, source="literal"),
                    source="literal",
                    raw_url=found,
                    confidence=0.6,
                )
            )

    try:
        tree = ast.parse(raw)
    except SyntaxError as exc:
        meta = RecipeMetadata(
            recipe_uid="",
            title=None,
            author=None,
            description=None,
            language=None,
            publication_type=None,
            needs_subscription=None,
            class_name=None,
        )
        return meta, url_hits, f"SyntaxError: {exc}"

    constants = _collect_module_constants(tree)
    class_node = _find_recipe_class(tree)
    class_name = class_node.name if class_node else None
    assigns = _extract_class_assignments(class_node, constants) if class_node else {}

    title = assigns.get("title")
    author = assigns.get("__author__")
    description = assigns.get("description")
    language = assigns.get("language")
    publication_type = assigns.get("publication_type")
    needs_subscription = assigns.get("needs_subscription")

    def _as_str(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", "replace")
        if isinstance(value, str):
            return value
        return str(value)

    meta = RecipeMetadata(
        recipe_uid="",
        title=_as_str(title),
        author=_as_str(author),
        description=_as_str(description),
        language=_as_str(language),
        publication_type=_as_str(publication_type),
        needs_subscription=_as_str(needs_subscription),
        class_name=class_name,
    )

    url_hits.extend(_feeds_to_endpoints(assigns.get("feeds")))
    url_hits.extend(_attr_url_hits(assigns))

    return meta, url_hits, None
