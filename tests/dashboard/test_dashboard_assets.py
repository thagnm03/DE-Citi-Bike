from __future__ import annotations

from pathlib import Path

from serving.api import app


ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = ROOT / "serving" / "dashboard"


def test_dashboard_is_self_contained_and_has_operational_landmarks() -> None:
    html = (DASHBOARD / "index.html").read_text(encoding="utf-8")
    assert "https://" not in html and "http://" not in html
    for marker in (
        'id="metric-active"',
        'id="alert-rows"',
        'id="station-rows"',
        'id="alert-dialog"',
        'id="queue-panel"',
        'aria-live="polite"',
    ):
        assert marker in html


def test_dashboard_javascript_uses_serving_endpoints_and_safe_rendering() -> None:
    javascript = (DASHBOARD / "app.js").read_text(encoding="utf-8")
    for endpoint in (
        "/health/ready",
        "/api/v1/alerts",
        "/api/v1/stations",
        "/acknowledge",
    ):
        assert endpoint in javascript
    assert "escapeHtml" in javascript
    assert "crypto.randomUUID()" in javascript
    assert "window.setInterval(refreshData, 30_000)" in javascript


def test_dashboard_styles_are_responsive_and_keyboard_visible() -> None:
    css = (DASHBOARD / "styles.css").read_text(encoding="utf-8")
    assert "@media (max-width: 900px)" in css
    assert "@media (max-width: 580px)" in css
    assert ":focus-visible" in css
    assert ".skip-link:focus" in css


def test_fastapi_exposes_dashboard_and_static_mount() -> None:
    paths = {getattr(route, "path", None) for route in app.routes}
    assert "/dashboard" in paths
    assert "/dashboard/static" in paths
