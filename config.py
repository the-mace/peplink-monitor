"""Shared config loading for peplink-monitor scripts."""

from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_DIR = Path(__file__).resolve().parent

# Default remote layout when both machines mirror the same repo path under $HOME.
_DEFAULT_REMOTE_PATH = "~/Documents/Code/peplink-monitor"


def load_config() -> dict:
    """Load config.yaml and resolve local paths relative to the project directory.

    Remote SSH helpers use ``remote_path`` / ``remote_db_path`` so laptop and Mini
    need not share identical absolute paths.
    """
    with open(PROJECT_DIR / "config.yaml") as fh:
        cfg = yaml.safe_load(fh) or {}

    db_path = Path(cfg.get("db_path", "data/monitor.db"))
    if not db_path.is_absolute():
        db_path = PROJECT_DIR / db_path
    cfg["db_path"] = str(db_path)

    report_dir = Path(cfg.get("report_dir", "reports"))
    if not report_dir.is_absolute():
        report_dir = PROJECT_DIR / report_dir
    cfg["report_dir"] = str(report_dir)

    # Remote paths: keep tilde form for SSH shells; expand only for local use.
    cfg.setdefault("remote_path", _DEFAULT_REMOTE_PATH)
    cfg.setdefault("remote_db_path", "data/monitor.db")
    cfg.setdefault("router_timezone", "America/New_York")
    cfg.setdefault("raw_retention_days", 0)  # 0 = keep forever

    return cfg
