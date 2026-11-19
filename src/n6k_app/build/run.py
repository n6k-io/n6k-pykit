from pathlib import Path
from typing import Any

from fastapi import FastAPI


def _config_fn(project_dir: Path, config: dict[str, Any], app: FastAPI | None = None) -> None:
    # `databases` (endpoint -> aliases it serves) is build-time only: the JS side
    # resolves each page's `attach:` frontmatter against it, errors on a name no
    # endpoint exposes, and bakes the resolved `attach` prop per page. A Database has
    # no URL of its own; the client names the catalog it wants in the FT_OPEN frame,
    # over its endpoint's /<slug>/ws socket.
    from n6k_app.database import catalogs_by_slug

    app_dir_path = Path(config.get("appDir", "src/app"))
    app_pkg = app_dir_path.name
    config["databases"] = catalogs_by_slug(app_dir_path, app_pkg) if app is not None else {}

    entry_points = getattr(app.state, "_n6k_entry_points", {}) if app is not None else {}
    root_props_by_slug: dict[str, dict[str, Any]] = {}
    for ep_key in entry_points:  # e.g. "pages/weather"
        slug = ep_key.split("/", 1)[1] if "/" in ep_key else ep_key
        # `slug` names this page's own endpoint: its UI socket, and the default
        # source of its catalogs without `attach:`.
        root_props_by_slug[ep_key] = {"slug": slug}

    config["rootTSX"] = "./root.tsx"
    config["rootProps"] = {}
    config["rootPropsBySlug"] = root_props_by_slug

    # Nav metadata for server-driven (.py) pages, read from their module-level
    # title/section/nav_order at discovery. The JS build seeds these into the same
    # `meta` map .mdx frontmatter uses, so routes/layout treat them identically.
    page_meta = getattr(app.state, "_n6k_page_meta", {}) if app is not None else {}
    if page_meta:
        config["meta"] = page_meta

    # Emit <appDir>/n6k.gen.ts so chart/stat bindings type-check against the
    # app's schema. Auxiliary to the build — a failure here must not break it.
    from n6k_app.build.typegen import generate_types

    app_dir_rel = config.get("appDir", "src/app")
    try:
        generate_types(project_dir, app, app_dir_rel)
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning("n6k.gen.ts generation skipped: %s", e)
