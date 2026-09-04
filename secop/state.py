"""Estado del ETL (watermark incremental) persistido en el object store.

Reemplaza a ``src/state.py``, que guardaba en disco local. Ahora vive junto a
los datos (``state/contratos.json`` en R2) para que el runner de GitHub Actions
sea sin estado.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from secop.config import DEFAULT_START_YEAR, STATE_KEY
from secop.storage.base import ObjectStore

logger = logging.getLogger(__name__)

_DEFAULT_WATERMARK = datetime(DEFAULT_START_YEAR, 1, 1, tzinfo=UTC)


@dataclass(slots=True)
class PipelineState:
    """Contenido de ``state/contratos.json``."""

    ultima_actualizacion: str = _DEFAULT_WATERMARK.isoformat()
    last_run_utc: str | None = None
    row_counts: dict[str, int] = field(default_factory=dict)
    schema_version: str | None = None

    @property
    def watermark(self) -> datetime:
        """``ultima_actualizacion`` como ``datetime`` con tz."""
        dt = datetime.fromisoformat(self.ultima_actualizacion)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def load_state(store: ObjectStore) -> PipelineState:
    """Carga el estado; devuelve el default si el objeto aún no existe."""
    try:
        raw = store.get_bytes(STATE_KEY)
    except KeyError:
        logger.info(
            "Sin estado previo en %s/%s; se usa el default", store.label, STATE_KEY
        )
        return PipelineState()
    data = json.loads(raw.decode("utf-8"))
    known = {f for f in PipelineState.__dataclass_fields__}
    return PipelineState(**{k: v for k, v in data.items() if k in known})


def save_state(store: ObjectStore, state: PipelineState) -> None:
    """Persiste el estado (marca ``last_run_utc`` con la hora actual)."""
    state.last_run_utc = datetime.now(UTC).isoformat()
    payload = json.dumps(asdict(state), indent=2, ensure_ascii=False).encode("utf-8")
    store.put_bytes(STATE_KEY, payload)
    logger.info("Watermark guardado: %s", state.ultima_actualizacion)
