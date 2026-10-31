"""Read n6k.config.json from the project root."""

import json
from pathlib import Path
from typing import Any

DEFAULT_SRC_DIR = "src"
DEFAULT_APP_DIR = "src/app"
DEFAULT_PORT = 7823
CONFIG_FILENAME = "n6k.config.json"


def load_config(project_dir: Path | str = ".") -> dict[str, Any]:
    """Load n6k.config.json from *project_dir*, returning {} if absent."""
    path = Path(project_dir) / CONFIG_FILENAME
    if not path.is_file():
        return {}
    result: dict[str, Any] = json.loads(path.read_text())
    return result


def get_src_dir(project_dir: Path | str = ".") -> Path:
    cfg = load_config(project_dir)
    return Path(cfg.get("srcDir", DEFAULT_SRC_DIR))


def get_app_dir(project_dir: Path | str = ".") -> Path:
    cfg = load_config(project_dir)
    return Path(cfg.get("appDir", DEFAULT_APP_DIR))


def get_port(project_dir: Path | str = ".") -> int:
    cfg = load_config(project_dir)
    return int(cfg.get("port", DEFAULT_PORT))


def init_config(project_dir: Path | str = ".") -> Path:
    project_dir = Path(project_dir)
    config_path = project_dir / CONFIG_FILENAME
    if not config_path.is_file():
        config = {
            "srcDir": DEFAULT_SRC_DIR,
            "appDir": DEFAULT_APP_DIR,
            "port": DEFAULT_PORT,
        }
        config_path.write_text(json.dumps(config, indent=2) + "\n")
    return config_path
