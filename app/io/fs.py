# hzcp/io/fs.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def iter_worker_logs(root: Path) -> Iterable[Path]:
    """
    Yield all files named 'worker.log' under root (recursive),
    but only if their parent directory name ends with '-member'.
    """
    root = root.resolve()

    for dirpath, _, filenames in os.walk(root):
        if not dirpath.endswith("-member"):
            continue

        for fn in filenames:
            if fn == "worker.log":
                yield Path(dirpath) / fn