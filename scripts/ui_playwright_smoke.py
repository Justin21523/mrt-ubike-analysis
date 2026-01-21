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
        route.fulfill(status=200, content_type="text/css", body="/* stub leaflet */")
        return
    js = """
      (function(){
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
        function Map(){ this._center=[0,0]; this._z=12; this._handlers={}; }
        Map.prototype.setView=function(c,z){ this._center=c; this._z=z; return this; };
        Map.prototype.on=function(ev, fn){ this._handlers[ev]=fn; return this; };
        Map.prototype.getCenter=function(){ return { lat:this._center[0], lng:this._center[1] }; };
        Map.prototype.getZoom=function(){ return this._z; };
        window.L = {
          map: function(){ return new Map(); },
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


def _iter_shots() -> Iterator[PageShot]:
    yield PageShot("home", "/home", "#homeCards")
    yield PageShot("insights", "/insights", "#insightsCards")
    yield PageShot("explorer", "/explorer", "#map")
    yield PageShot("ops", "/ops", "#opsCards")
    yield PageShot("about", "/about", "main.page")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--out-dir", default=str(_repo_root() / "docs" / "screenshots"))
    ap.add_argument("--start-api", action="store_true", help="Start API via scripts/run_api.py")
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

            # Make the run deterministic: do not depend on CDN availability.
            ctx.route("https://unpkg.com/leaflet@*/dist/leaflet.css", _stub_leaflet)
            ctx.route("https://unpkg.com/leaflet@*/dist/leaflet.js", _stub_leaflet)
            ctx.route("https://cdn.jsdelivr.net/npm/chart.js@*/dist/chart.umd.min.js", _stub_chartjs)

            for shot in _iter_shots():
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

