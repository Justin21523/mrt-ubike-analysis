from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import requests
from playwright.sync_api import Route, sync_playwright


@dataclass(frozen=True)
class PageShot:
    name: str
    path: str
    wait_for: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _wait_http_ok(url: str, timeout_s: float = 30.0) -> None:
    start = time.time()
    last_err: Exception | None = None
    while time.time() - start < timeout_s:
        try:
            r = requests.get(url, timeout=2.0)
            if r.status_code < 500:
                return
        except Exception as e:  # noqa: BLE001 - best-effort wait
            last_err = e
        time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {url}. Last error: {last_err}")


def _start_api(repo_root: Path, base_url: str) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env.setdefault("METROBIKEATLAS_DEMO_MODE", "false")
    env.setdefault("METROBIKEATLAS_HOST", "127.0.0.1")
    env.setdefault("METROBIKEATLAS_PORT", base_url.rsplit(":", 1)[-1])
    return subprocess.Popen(  # noqa: S603,S607 - controlled local command
        [sys.executable, "scripts/run_api.py"],
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _terminate(proc: subprocess.Popen[bytes], timeout_s: float = 8.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=timeout_s)
        return
    except Exception:  # noqa: BLE001
        pass
    try:
        proc.terminate()
        proc.wait(timeout=timeout_s)
        return
    except Exception:  # noqa: BLE001
        pass
    try:
        proc.kill()
        proc.wait(timeout=timeout_s)
    except Exception:  # noqa: BLE001
        pass


def _stub_leaflet(route: Route) -> None:
    url = route.request.url
    if url.endswith(".css"):
        css = """
        /* Minimal Leaflet stub so the Explorer page shows a visible "map" in screenshots. */
        .leaflet-container { position: relative; overflow: hidden; background: #f3f4f6; }
        .leaflet-pane { position: absolute; top: 0; left: 0; right: 0; bottom: 0; }
        .leaflet-tile-pane { position: absolute; top: 0; left: 0; right: 0; bottom: 0; }
        .leaflet-tile {
          position: absolute;
          width: 256px;
          height: 256px;
          background: repeating-linear-gradient(0deg, #e5e7eb, #e5e7eb 1px, #f9fafb 1px, #f9fafb 32px),
                      repeating-linear-gradient(90deg, #e5e7eb, #e5e7eb 1px, transparent 1px, transparent 32px);
          opacity: 0.95;
        }
        .leaflet-control-container { display: none; }
        """
        route.fulfill(status=200, content_type="text/css", body=css)
        return
    js = """
      (function(){
        function ensureDom(el){
          if (!el) return;
          el.classList.add('leaflet-container');
          el.setAttribute('data-leaflet-ready', '1');
          if (el.querySelector('.leaflet-pane')) return;
          var pane = document.createElement('div');
          pane.className = 'leaflet-pane';
          var tilePane = document.createElement('div');
          tilePane.className = 'leaflet-tile-pane';
          var tile = document.createElement('div');
          tile.className = 'leaflet-tile leaflet-tile-loaded';
          tile.style.left = '0px';
          tile.style.top = '0px';
          tilePane.appendChild(tile);
          pane.appendChild(tilePane);
          el.appendChild(pane);
        }
        function Layer(){ this._layers=[]; }
        Layer.prototype.addTo=function(){ return this; };
        Layer.prototype.clearLayers=function(){ this._layers=[]; };
        Layer.prototype.getBounds=function(){ return { isValid: function(){ return false; } }; };
        function Marker(){ this._style={}; this._tooltip=''; this._handlers={}; }
        Marker.prototype.setStyle=function(s){ this._style = Object.assign(this._style||{}, s||{}); };
        Marker.prototype.bindTooltip=function(html){ this._tooltip = html; return this; };
        Marker.prototype.setTooltipContent=function(html){ this._tooltip = html; return this; };
        Marker.prototype.on=function(ev, fn){ this._handlers[ev]=fn; return this; };
        Marker.prototype.addTo=function(){ return this; };
        function Map(el){ this._el=el; this._center=[0,0]; this._z=12; this._handlers={}; ensureDom(el); }
        Map.prototype.setView=function(c,z){ this._center=c; this._z=z; ensureDom(this._el); return this; };
        Map.prototype.on=function(ev, fn){ this._handlers[ev]=fn; return this; };
        Map.prototype.getCenter=function(){ return { lat:this._center[0], lng:this._center[1] }; };
        Map.prototype.getZoom=function(){ return this._z; };
        window.L = {
          map: function(idOrEl){
            var el = (typeof idOrEl === 'string') ? document.getElementById(idOrEl) : idOrEl;
            return new Map(el);
          },
          tileLayer: function(){ return new Layer(); },
          layerGroup: function(){ return new Layer(); },
          circleMarker: function(){ return new Marker(); },
        };
      })();
    """
    route.fulfill(status=200, content_type="application/javascript", body=js)


def _stub_chartjs(route: Route) -> None:
    js = """
      (function(){
        function Chart(){ this.data={labels:[],datasets:[{data:[]}]} ; this.options={ scales: { x: { ticks: {} }, y: { ticks: {} } } }; }
        Chart.prototype.update=function(){};
        window.Chart = Chart;
      })();
    """
    route.fulfill(status=200, content_type="application/javascript", body=js)


def _stub_osm_tiles(route: Route) -> None:
    # Deterministic placeholder: avoids network dependency while still rendering a "map-like" base layer.
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256">
      <defs>
        <pattern id="grid" width="32" height="32" patternUnits="userSpaceOnUse">
          <rect width="32" height="32" fill="#f9fafb"/>
          <path d="M 32 0 L 0 0 0 32" fill="none" stroke="#e5e7eb" stroke-width="1"/>
        </pattern>
      </defs>
      <rect width="256" height="256" fill="url(#grid)"/>
      <path d="M0 128 H256 M128 0 V256" stroke="#d1d5db" stroke-width="2" opacity="0.7"/>
    </svg>"""
    route.fulfill(status=200, content_type="image/svg+xml", body=svg)


def _iter_shots() -> Iterator[PageShot]:
    yield PageShot("home", "/home", "#homeCards")
    yield PageShot("insights", "/insights", "#insightsCards")
    # Wait for actual Leaflet render (real or stubbed) so screenshots include a visible map.
    yield PageShot("explorer", "/explorer", "#map .leaflet-tile")
    yield PageShot("ops", "/ops", "#opsCards")
    yield PageShot("about", "/about", "main.page")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--out-dir", default=str(_repo_root() / "docs" / "screenshots"))
    ap.add_argument("--start-api", action="store_true", help="Start API via scripts/run_api.py")
    ap.add_argument(
        "--pages",
        default="home,insights,explorer,ops,about",
        help="Comma-separated page names to capture (home,insights,explorer,ops,about)",
    )
    ap.add_argument(
        "--stub-leaflet",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stub Leaflet assets (offline/deterministic; use --no-stub-leaflet to load real Leaflet)",
    )
    ap.add_argument(
        "--stub-chartjs",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stub Chart.js (deterministic; use --no-stub-chartjs to load real Chart.js)",
    )
    ap.add_argument("--timeout-s", type=float, default=60.0)
    args = ap.parse_args()

    repo_root = _repo_root()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    proc: subprocess.Popen[bytes] | None = None
    try:
        if args.start_api:
            proc = _start_api(repo_root, args.base_url)
            _wait_http_ok(f"{args.base_url}/status", timeout_s=min(args.timeout_s, 45.0))

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1)

            # Make the run deterministic: avoid network dependency where possible.
            if args.stub_leaflet:
                ctx.route("https://unpkg.com/leaflet@*/dist/leaflet.css", _stub_leaflet)
                ctx.route("https://unpkg.com/leaflet@*/dist/leaflet.js", _stub_leaflet)
            if args.stub_chartjs:
                ctx.route("https://cdn.jsdelivr.net/npm/chart.js@*/dist/chart.umd.min.js", _stub_chartjs)
            # Tile servers are frequently rate-limited; stub to a placeholder tile.
            ctx.route("https://*.tile.openstreetmap.org/*", _stub_osm_tiles)
            ctx.route("https://tile.openstreetmap.org/*", _stub_osm_tiles)

            wanted = {x.strip() for x in str(args.pages).split(",") if x.strip()}
            for shot in _iter_shots():
                if wanted and shot.name not in wanted:
                    continue
                page = ctx.new_page()
                page.goto(f"{args.base_url}{shot.path}", wait_until="domcontentloaded", timeout=int(args.timeout_s * 1000))
                page.wait_for_selector(shot.wait_for, timeout=int(args.timeout_s * 1000))
                page.wait_for_timeout(400)
                page.screenshot(path=str(out_dir / f"{shot.name}.png"), full_page=True)
                page.close()

            ctx.close()
            browser.close()

        print(f"Wrote screenshots to {out_dir}")
        return 0
    finally:
        if proc is not None:
            _terminate(proc)


if __name__ == "__main__":
    raise SystemExit(main())
