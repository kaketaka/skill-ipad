from __future__ import annotations

from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from market_sim.db import init_db
from market_sim.service import get_dashboard, reset_demo_state, run_market_cycle, run_review, sync_universe, update_settings


ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"

app = Flask(__name__, static_folder=str(STATIC), static_url_path="")


@app.get("/")
def index():
    return send_from_directory(STATIC, "index.html")


@app.get("/api/status")
def api_status():
    return jsonify(get_dashboard())


@app.post("/api/run")
def api_run():
    payload = request.get_json(silent=True) or {}
    markets = payload.get("markets") or ["US", "JP"]
    return jsonify(run_market_cycle(markets))


@app.post("/api/review")
def api_review():
    return jsonify(run_review())


@app.post("/api/settings")
def api_settings():
    payload = request.get_json(silent=True) or {}
    return jsonify(update_settings(payload))


@app.post("/api/sync-universe")
def api_sync_universe():
    payload = request.get_json(silent=True) or {}
    markets = payload.get("markets") or ["US", "JP"]
    return jsonify(sync_universe(markets))


@app.post("/api/reset-demo")
def api_reset_demo():
    payload = request.get_json(silent=True) or {}
    return jsonify(reset_demo_state(keep_signals=bool(payload.get("keep_signals", True))))


@app.get("/health")
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8765, debug=False)
