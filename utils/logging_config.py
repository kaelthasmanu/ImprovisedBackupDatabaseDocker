from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

DEFAULT_FORMAT = '%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s'

def init_logging(level: int = logging.INFO, *, log_dir: str = 'logs', filename: Optional[str] = None,
                 fmt: str = DEFAULT_FORMAT) -> str:
    root = logging.getLogger()
    if root.handlers:
        # Already configured; try to find FileHandler path
        for h in root.handlers:
            if isinstance(h, logging.FileHandler):
                return getattr(h, 'baseFilename', '')
        return ''

    os.makedirs(log_dir, exist_ok=True)
    if not filename:
        filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    log_path = os.path.join(log_dir, filename)

    root.setLevel(level)
    formatter = logging.Formatter(fmt)

    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setFormatter(formatter)
    root.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    root.addHandler(ch)

    return log_path

def get_logger(name: Optional[str] = None) -> logging.Logger:
    init_logging()  # idempotent
    return logging.getLogger(name)

__all__ = ["init_logging", "get_logger"]
