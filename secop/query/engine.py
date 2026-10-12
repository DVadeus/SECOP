"""Sesión DuckDB sobre el data lake Parquet (R2 o local)."""

from __future__ import annotations

import logging
from pathlib import Path
from types import TracebackType

import duckdb
import pandas as pd

from secop.bases import BASES, CONTRATOS, Base
from secop.config import Settings, get_settings

logger = logging.getLogger(__name__)

VIEW_NAME = CONTRATOS.name
HISTORY_VIEW_NAME = CONTRATOS.history_view


def _q(value: str) -> str:
    """Escapa un literal SQL de una comilla simple."""
    return value.replace("'", "''")


class QueryEngine:
    """Abre una conexión DuckDB y expone una vista por base y su historial.

    Vistas: ``contratos`` e ``interadministrativos`` (cada una lee sólo su
    archivo de las carpetas ``year=*``) y, si hay versiones reemplazadas,
    ``contratos_historial`` e ``interadministrativos_historial``. Una vista sólo
    existe si hay archivos (ver ``has_view``).

    Úsese como context manager::

        with QueryEngine.from_source("r2") as qe:
            df = qe.run("SELECT count(*) FROM contratos")
    """

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        globs: dict[str, str],
        history_globs: dict[str, str] | None = None,
    ) -> None:
        self._con = connection
        self.globs = dict(globs)
        self.history_globs = dict(history_globs or {})
        self.views: set[str] = set()
        for base_name, glob in self.globs.items():
            self._create_view_if_files(BASES[base_name].name, glob)
        for base_name, glob in self.history_globs.items():
            self._create_view_if_files(BASES[base_name].history_view, glob)

    @property
    def glob(self) -> str:
        """Glob de la base de contratos (compatibilidad)."""
        return self.globs[CONTRATOS.name]

    @property
    def has_history(self) -> bool:
        """Hay historial de contratos (compatibilidad; ver ``has_view``)."""
        return HISTORY_VIEW_NAME in self.views

    def has_view(self, name: str) -> bool:
        return name in self.views

    def require_view(self, name: str) -> None:
        """Error claro si una vista no existe (la base aún no tiene datos)."""
        if name not in self.views:
            raise LookupError(f"No hay datos para la vista {name!r} en este origen")

    def _create_view_if_files(self, name: str, glob: str) -> None:
        if not self._glob_has_files(glob):
            logger.debug("Sin archivos para la vista %s (%s)", name, glob)
            return
        self._con.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_parquet('{_q(glob)}', "
            f"hive_partitioning => true, union_by_name => true)"
        )
        self.views.add(name)

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
            globs, history = cls._globs_r2(root)
        elif source == "local":
            globs, history = cls._globs_local(settings, path)
        else:
            raise ValueError(f"source desconocido: {source!r} (usa 'r2' o 'local')")
        return cls(con, globs, history)

    @staticmethod
    def _globs_r2(root: str) -> tuple[dict[str, str], dict[str, str]]:
        globs = {
            b.name: f"{root}/{b.lake_prefix}/year=*/{b.data_file}"
            for b in BASES.values()
        }
        history = {
            b.name: f"{root}/{b.history_prefix}/detectado=*/{b.data_file}"
            for b in BASES.values()
        }
        return globs, history

    @staticmethod
    def _globs_local(
        settings: Settings, path: str | Path | None
    ) -> tuple[dict[str, str], dict[str, str]]:
        # Sólo el archivo compactado de cada base en cada "year=..."; así se
        # ignoran los part-*.parquet y carpetas sin partición Hive del ETL viejo
        # (p.ej. data/raw/nulls/). Con --path se busca recursivamente.
        def root(prefix: str) -> str:
            base_dir = Path(path) if path else settings.local_data_dir / prefix
            return base_dir.as_posix().rstrip("/")

        globs = {
            b.name: f"{root(b.lake_prefix)}/**/year=*/{b.data_file}"
            for b in BASES.values()
        }
        history = {
            b.name: f"{root(b.history_prefix)}/**/detectado=*/{b.data_file}"
            for b in BASES.values()
        }
        return globs, history

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

    def count(self, base: Base = CONTRATOS) -> int:
        self.require_view(base.name)
        value = self.run(f"SELECT count(*) AS n FROM {base.name}").iat[0, 0]
        return int(value)  # pyright: ignore[reportArgumentType]

    def partition_summary(self, base: Base = CONTRATOS) -> pd.DataFrame:
        self.require_view(base.name)
        return self.run(
            f"SELECT year, count(*) AS filas FROM {base.name} "
            f"GROUP BY year ORDER BY year"
        )

    def date_range(
        self, base: Base = CONTRATOS
    ) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
        self.require_view(base.name)
        col = base.partition_date_column
        row = self.run(
            f"SELECT min({col}) AS lo, max({col}) AS hi FROM {base.name}"
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
