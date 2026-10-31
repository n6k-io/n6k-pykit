"""Uvicorn import target for ``n6k database --reload``. The CLI sets N6K_DB_FILE; a
reload re-imports the file."""

import os

from n6k_app.database_app import database_app

app = database_app(os.environ["N6K_DB_FILE"])
