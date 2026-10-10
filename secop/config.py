"""Configuración central del proyecto (Socrata + Cloudflare R2 + rutas)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Raíz del repositorio (dos niveles arriba de este archivo: secop/config.py -> repo/).
REPO_ROOT = Path(__file__).resolve().parent.parent

# Dataset SECOP II "Contratos Electrónicos" en datos.gov.co (API Socrata).
DATASET_ID = "jbjy-vk9h"
SOCRATA_RESOURCE_URL = f"https://www.datos.gov.co/resource/{DATASET_ID}.json"

# Clave primaria y columna incremental del dataset (ver memory-bank/datasetContract.md).
PRIMARY_KEY = "id_contrato"
WATERMARK_COLUMN = "ultima_actualizacion"
PARTITION_DATE_COLUMN = "fecha_de_inicio_del_contrato"

# Prefijos dentro del bucket / directorio de datos.
LAKE_PREFIX = "raw/contratos"
# Único archivo por partición (contrato: nunca part-*.parquet).
DATA_FILE = "data.parquet"
STATE_KEY = "state/contratos.json"
MANIFEST_KEY = "_manifest.json"

# Primer año con datos utilizables en el dataset.
DEFAULT_START_YEAR = 2018


class Settings(BaseSettings):
    """Variables de entorno. Se cargan desde el entorno del proceso o desde ``.env``."""

    model_config = SettingsConfigDict(
        env_file=str(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Socrata -------------------------------------------------------------
    # Token de aplicación de datos.gov.co. Acepta el nombre nuevo y el heredado.
    socrata_app_token: str | None = Field(
        default=None,
        validation_alias="SOCRATA_APP_TOKEN",
    )
    id_token_legacy: str | None = Field(default=None, validation_alias="ID_TOKEN")
    socrata_page_size: int = Field(default=50_000, validation_alias="SOCRATA_PAGE_SIZE")

    # --- Cloudflare R2 (API S3) -------------------------------------------------
    r2_account_id: str | None = Field(default=None, validation_alias="R2_ACCOUNT_ID")
    r2_access_key_id: str | None = Field(
        default=None, validation_alias="R2_ACCESS_KEY_ID"
    )
    r2_secret_access_key: str | None = Field(
        default=None, validation_alias="R2_SECRET_ACCESS_KEY"
    )
    r2_bucket: str = Field(default="secop", validation_alias="R2_BUCKET")
    r2_endpoint: str | None = Field(default=None, validation_alias="R2_ENDPOINT")

    # --- Local ------------------------------------------------------------------
    # Raíz del lake local (espejo de la estructura de R2). data/raw/ es la caché
    # del ETL viejo (part-*.parquet sin compactar): no usarla como lake.
    local_data_dir: Path = Field(
        default=REPO_ROOT / "data" / "lake",
        validation_alias="SECOP_LOCAL_DATA_DIR",
    )

    @property
    def app_token(self) -> str | None:
        """Token de Socrata efectivo (nuevo nombre con prioridad sobre el heredado)."""
        return self.socrata_app_token or self.id_token_legacy

    @property
    def r2_endpoint_url(self) -> str | None:
        """Endpoint S3 de R2, derivado del account id si no se define explícitamente."""
        if self.r2_endpoint:
            return self.r2_endpoint
        if self.r2_account_id:
            return f"https://{self.r2_account_id}.r2.cloudflarestorage.com"
        return None

    def require_r2(self) -> None:
        """Falla temprano con un mensaje claro si falta configuración de R2."""
        missing = [
            name
            for name, value in {
                "R2_ACCOUNT_ID": self.r2_account_id,
                "R2_ACCESS_KEY_ID": self.r2_access_key_id,
                "R2_SECRET_ACCESS_KEY": self.r2_secret_access_key,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Faltan variables de entorno de Cloudflare R2: "
                + ", ".join(missing)
                + ". Configúralas en .env o como repository secrets."
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Devuelve la instancia única de ``Settings`` (cacheada)."""
    return Settings()
