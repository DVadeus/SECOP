"""Sesión DuckDB sobre el data lake Parquet (R2 o local)."""

from __future__ import annotations

import logging
from pathlib import Path
from types import TracebackType

import duckdb
import pandas as pd

from secop.config import (
    DATA_FILE,
    HISTORY_PREFIX,
    LAKE_PREFIX,
    Settings,
    get_settings,
)

logger = logging.getLogger(__name__)

VIEW_NAME = "contratos"
HISTORY_VIEW_NAME = "contratos_historial"


def _q(value: str) -> str:
    """Escapa un literal SQL de una comilla simple."""
    return value.replace("'", "''")


class QueryEngine:
    """Abre una conexión DuckDB y expone la vista ``contratos``.

    Si hay historial de versiones, expone además ``contratos_historial``
    (ver ``has_history``).

    Úsese como context manager::

        with QueryEngine.from_source("r2") as qe:
            df = qe.run("SELECT count(*) FROM contratos")
    """

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        glob: str,
        history_glob: str | None = None,
    ) -> None:
        self._con = connection
        self.glob = glob
        self.history_glob = history_glob
        self._create_view(VIEW_NAME, glob)
        self.has_history = bool(history_glob) and self._glob_has_files(history_glob)
        if self.has_history and history_glob:
            self._create_view(HISTORY_VIEW_NAME, history_glob)

    def _create_view(self, name: str, glob: str) -> None:
        self._con.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_parquet('{_q(glob)}', "
            f"hive_partitioning => true, union_by_name => true)"
        )

    def _glob_has_files(self, glob: str) -> bool:
        row = self._con.execute("SELECT count(*) FROM glob(?)", [glob]).fetchone()
        return bool(row and row[0])

    # -- constructores ----------------------------------------------------- #
    @classmethod
    def from_source(
        cls,
        source: str,
        *,
        settings: Settings | None = None,
        path: str | Path | None = None,
    ) -> QueryEngine:
        settings = settings or get_settings()
        con = duckdb.connect(database=":memory:")
        if source == "r2":
            root = cls._configure_r2(con, settings)
            glob = f"{root}/{LAKE_PREFIX}/year=*/{DATA_FILE}"
            history_glob = f"{root}/{HISTORY_PREFIX}/detectado=*/{DATA_FILE}"
        elif source == "local":
            # Sólo el data.parquet compactado de cada "year=..."; así se ignoran
            # los part-*.parquet y carpetas sin partición Hive del ETL viejo
            # (p.ej. data/raw/nulls/). Con --path se busca recursivamente.
            base = Path(path) if path else settings.local_data_dir / LAKE_PREFIX
            hist = Path(path) if path else settings.local_data_dir / HISTORY_PREFIX
            glob = f"{base.as_posix().rstrip('/')}/**/year=*/{DATA_FILE}"
            history_glob = f"{hist.as_posix().rstrip('/')}/**/detectado=*/{DATA_FILE}"
        else:
            raise ValueError(f"source desconocido: {source!r} (usa 'r2' o 'local')")
        return cls(con, glob, history_glob)

    @staticmethod
    def _configure_r2(con: duckdb.DuckDBPyConnection, settings: Settings) -> str:
        """Configura el acceso a R2 y devuelve la raíz del bucket (r2:// o s3://)."""
        settings.require_r2()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        account = _q(settings.r2_account_id or "")
        key = _q(settings.r2_access_key_id or "")
        secret = _q(settings.r2_secret_access_key or "")
        try:
            con.execute(
                f"CREATE OR REPLACE SECRET secop_r2 (TYPE r2, "
                f"ACCOUNT_ID '{account}', KEY_ID '{key}', SECRET '{secret}')"
            )
            return f"r2://{settings.r2_bucket}"
        except duckdb.Error as exc:  # pragma: no cover - fallback poco común
            logger.warning("SECRET TYPE r2 falló (%s); se usa TYPE s3", exc)
            endpoint = _q((settings.r2_endpoint_url or "").removeprefix("https://"))
            con.execute(
                f"CREATE OR REPLACE SECRET secop_r2 (TYPE s3, "
                f"KEY_ID '{key}', SECRET '{secret}', "
                f"ENDPOINT '{endpoint}', URL_STYLE 'path', REGION 'auto')"
            )
            return f"s3://{settings.r2_bucket}"

    # -- API -------------------------------------------------------------- #
    def run(self, sql: str, params: list[object] | None = None) -> pd.DataFrame:
        """Ejecuta SQL y devuelve un DataFrame."""
        logger.debug("SQL: %s", sql)
        return self._con.execute(sql, params or []).df()

    def count(self) -> int:
        value = self.run(f"SELECT count(*) AS n FROM {VIEW_NAME}").iat[0, 0]
        return int(value)  # pyright: ignore[reportArgumentType]

    def partition_summary(self) -> pd.DataFrame:
        return self.run(
            f"SELECT year, count(*) AS filas FROM {VIEW_NAME} "
            f"GROUP BY year ORDER BY year"
        )

    def date_range(self) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
        row = self.run(
            f"SELECT min(fecha_de_inicio_del_contrato) AS lo, "
            f"max(fecha_de_inicio_del_contrato) AS hi FROM {VIEW_NAME}"
        ).iloc[0]
        return row["lo"], row["hi"]

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> QueryEngine:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        del exc_type, exc, tb
        self.close()
