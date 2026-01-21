from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PageSpec:
    path: Path
    required_ids: tuple[str, ...]
    required_css: tuple[str, ...]
    required_scripts: tuple[str, ...]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _has_id(html: str, element_id: str) -> bool:
    return bool(re.search(rf'id="{re.escape(element_id)}"', html))


def _has_href(html: str, href_prefix: str) -> bool:
    return href_prefix in html


def validate_page(spec: PageSpec) -> list[str]:
    errors: list[str] = []
    html = _read_text(spec.path)

    for element_id in spec.required_ids:
        if not _has_id(html, element_id):
            errors.append(f"{spec.path}: missing id={element_id!r}")

    for href in spec.required_css:
        if not _has_href(html, href):
            errors.append(f"{spec.path}: missing CSS link containing {href!r}")

    for script in spec.required_scripts:
        if not _has_href(html, script):
            errors.append(f"{spec.path}: missing script containing {script!r}")

    return errors


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    web = repo_root / "web"

    css = (
        "/static/theme.css",
        "/static/layout.css",
        "/static/components.css",
        "/static/cards.css",
    )

    specs = [
        PageSpec(
            path=web / "home.html",
            required_ids=("homeCards", "statusText"),
            required_css=css,
            required_scripts=("/static/home.js",),
        ),
        PageSpec(
            path=web / "insights.html",
            required_ids=("insightsCards", "statusText"),
            required_css=css,
            required_scripts=("/static/insights.js",),
        ),
        PageSpec(
            path=web / "ops.html",
            required_ids=("opsCards", "statusText", "loadingOverlay", "overlayText"),
            required_css=css,
            required_scripts=("/static/ops.js",),
        ),
        PageSpec(
            path=web / "about.html",
            required_ids=("statusText",),
            required_css=css,
            required_scripts=("/static/about.js",),
        ),
        PageSpec(
            path=web / "explorer.html",
            required_ids=("map", "leftPanel", "rightPanel", "loadingOverlay", "overlayText", "statusText"),
            required_css=css + ("/static/map.css", "/static/charts.css"),
            required_scripts=("/static/explorer/main.js",),
        ),
    ]

    errors: list[str] = []
    for s in specs:
        errors.extend(validate_page(s))

    if errors:
        for e in errors:
            print(e)
        return 1
    print("OK: UI pages contain expected assets/ids.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
