"""Abstracción de almacenamiento de objetos (R2 / filesystem local)."""

from __future__ import annotations

from secop.config import Settings, get_settings
from secop.storage.base import ObjectStore
from secop.storage.local import LocalStore
from secop.storage.r2 import R2Store

__all__ = ["ObjectStore", "LocalStore", "R2Store", "get_store"]


def get_store(target: str, settings: Settings | None = None) -> ObjectStore:
    """Fábrica de stores. ``target`` es ``"r2"`` o ``"local"``."""
    settings = settings or get_settings()
    if target == "r2":
        settings.require_r2()
        return R2Store(settings)
    if target == "local":
        return LocalStore(settings.local_data_dir)
    raise ValueError(f"target desconocido: {target!r} (usa 'r2' o 'local')")
