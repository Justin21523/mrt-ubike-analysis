from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests


def _wait_ok(url: str, timeout_s: float = 20.0) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            r = requests.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {url}")


@pytest.mark.skipif(os.getenv("RUN_UI_TESTS") != "1", reason="set RUN_UI_TESTS=1 to enable UI smoke tests")
def test_ui_pages_load(tmp_path: Path) -> None:
    pytest.importorskip("playwright.sync_api")

    base_url = "http://127.0.0.1:8001"
    env = os.environ.copy()
    env.setdefault("METROBIKEATLAS_DEMO_MODE", "true")
    env["METROBIKEATLAS_HOST"] = "127.0.0.1"
    env["METROBIKEATLAS_PORT"] = "8001"

    proc = subprocess.Popen(  # noqa: S603,S607 - local controlled command
        [sys.executable, "scripts/run_api.py"],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_ok(f"{base_url}/status")

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1200, "height": 800})

            # Stub external assets for determinism.
            ctx.route("https://unpkg.com/leaflet@*/dist/leaflet.css", lambda r: r.fulfill(status=200, content_type="text/css", body="/* stub */"))
            ctx.route("https://unpkg.com/leaflet@*/dist/leaflet.js", lambda r: r.fulfill(status=200, content_type="application/javascript", body="window.L={map:()=>({setView:()=>({}),on:()=>{},getCenter:()=>({lat:0,lng:0}),getZoom:()=>12}),tileLayer:()=>({addTo:()=>({})}),layerGroup:()=>({addTo:()=>({}),clearLayers:()=>{}}),circleMarker:()=>({setStyle:()=>{},bindTooltip:()=>({}),setTooltipContent:()=>{},on:()=>{},addTo:()=>({})})};"))
            ctx.route("https://cdn.jsdelivr.net/npm/chart.js@*/dist/chart.umd.min.js", lambda r: r.fulfill(status=200, content_type="application/javascript", body="window.Chart=function(){this.data={labels:[],datasets:[{data:[]}]};this.options={scales:{x:{ticks:{}},y:{ticks:{}}}};this.update=function(){};};"))

            pages = [
                ("/home", "#homeCards"),
                ("/insights", "#insightsCards"),
                ("/explorer", "#map"),
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
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            proc.kill()

