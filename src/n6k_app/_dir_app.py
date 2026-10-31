"""Uvicorn entry point for directory-based mode; each reload re-discovers the app."""

import os

from n6k_app.discover import discover_app

app_dir = os.environ["N6K_APP_DIR"]
app = discover_app(app_dir)
