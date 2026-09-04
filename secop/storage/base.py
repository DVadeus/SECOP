"""Protocolo común de almacenamiento de objetos."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ObjectStore(Protocol):
    """Operaciones mínimas sobre un almacén tipo clave -> bytes."""

    #: Etiqueta legible del backend ("r2://bucket" o una ruta local).
    label: str

    def put_bytes(self, key: str, data: bytes) -> None:
        """Escribe (o sobrescribe) el objeto en ``key``."""
        ...

    def get_bytes(self, key: str) -> bytes:
        """Devuelve el contenido de ``key``. Lanza ``KeyError`` si no existe."""
        ...

    def exists(self, key: str) -> bool:
        """Indica si ``key`` existe."""
        ...

    def list_keys(self, prefix: str) -> list[str]:
        """Lista las claves bajo ``prefix`` (recursivo)."""
        ...

    def delete(self, key: str) -> None:
        """Elimina ``key`` si existe (idempotente)."""
        ...

    def copy(self, src_key: str, dst_key: str) -> None:
        """Copia ``src_key`` a ``dst_key`` (sobrescribe el destino)."""
        ...
