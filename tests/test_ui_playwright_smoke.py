from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests


def _wait_ok(url: str, proc: subprocess.Popen[bytes], log_path: Path, timeout_s: float = 45.0) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        if proc.poll() is not None:
            out = ""
            try:
                out = log_path.read_text(errors="replace")[-4000:]
            except Exception:  # noqa: BLE001
                pass
            raise RuntimeError(f"API process exited (code={proc.returncode}). Output tail:\n{out}")
        try:
            r = requests.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.25)
    out = ""
    try:
        out = log_path.read_text(errors="replace")[-4000:]
    except Exception:  # noqa: BLE001
        pass
    raise RuntimeError(f"Timed out waiting for {url}. Output tail:\n{out}")


@pytest.mark.skipif(os.getenv("RUN_UI_TESTS") != "1", reason="set RUN_UI_TESTS=1 to enable UI smoke tests")
def test_ui_pages_load(tmp_path: Path) -> None:
    pytest.importorskip("playwright.sync_api")

    base_url = "http://127.0.0.1:8001"
    env = os.environ.copy()
    env["METROBIKEATLAS_DEMO_MODE"] = "true"
    env["METROBIKEATLAS_HOST"] = "127.0.0.1"
    env["METROBIKEATLAS_PORT"] = "8001"

    log_path = tmp_path / "api.log"
    log_f = log_path.open("wb")
    proc = subprocess.Popen(  # noqa: S603,S607 - local controlled command
        [sys.executable, "scripts/run_api.py"],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        stdout=log_f,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_ok(f"{base_url}/status", proc=proc, log_path=log_path)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1200, "height": 800})

            # Stub external assets for determinism.
            ctx.route(
                "https://unpkg.com/leaflet@*/dist/leaflet.css",
                lambda r: r.fulfill(
                    status=200,
                    content_type="text/css",
                    body="""
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
                    """,
                ),
            )
            ctx.route(
                "https://unpkg.com/leaflet@*/dist/leaflet.js",
                lambda r: r.fulfill(
                    status=200,
                    content_type="application/javascript",
                    body="""
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
                      Layer.prototype.addTo=function(map){ if (map && map._el) ensureDom(map._el); return this; };
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
                        circle: function(){ return new Marker(); },
                        polyline: function(){ return new Marker(); },
                      };
                    })();
                    """,
                ),
            )
            ctx.route("https://cdn.jsdelivr.net/npm/chart.js@*/dist/chart.umd.min.js", lambda r: r.fulfill(status=200, content_type="application/javascript", body="window.Chart=function(){this.data={labels:[],datasets:[{data:[]}]};this.options={scales:{x:{ticks:{}},y:{ticks:{}}}};this.update=function(){};};"))

            pages = [
                ("/home", "#homeCards"),
                ("/insights", "#insightsCards"),
                # Wait for the map to be visibly rendered (real or stubbed Leaflet) before considering the page "loaded".
                ("/explorer", "#map .leaflet-tile"),
                ("/ops", "#opsCards"),
                ("/about", "main.page"),
            ]
            for path, sel in pages:
                page = ctx.new_page()
                page.goto(f"{base_url}{path}", wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_selector(sel, timeout=30_000)
                page.close()

            ctx.close()
            browser.close()
    finally:
        try:
            log_f.close()
        except Exception:  # noqa: BLE001
            pass
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            proc.kill()
