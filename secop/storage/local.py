"""Almacén de objetos sobre el filesystem local (dev / tests / modo offline)."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class LocalStore:
    """Mapea claves estilo S3 a archivos bajo un directorio raíz."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.label = str(self.root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_bytes(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Escritura atómica: archivo temporal + rename en el mismo directorio.
        tmp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
        tmp.write_bytes(data)
        os.replace(tmp, path)
        logger.debug("Escrito local: %s (%d bytes)", path, len(data))

    def get_bytes(self, key: str) -> bytes:
        path = self._path(key)
        if not path.is_file():
            raise KeyError(key)
        return path.read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        search_root = base if base.is_dir() else base.parent
        if not search_root.exists():
            return []
        keys = [
            p.relative_to(self.root).as_posix()
            for p in search_root.rglob("*")
            if p.is_file()
        ]
        return sorted(k for k in keys if k.startswith(prefix))

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)

    def copy(self, src_key: str, dst_key: str) -> None:
        dst = self._path(dst_key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._path(src_key), dst)
