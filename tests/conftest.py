"""Fixtures compartidas."""

from __future__ import annotations

from pathlib import Path

import pytest

from secop.storage.local import LocalStore


@pytest.fixture
def store(tmp_path: Path) -> LocalStore:
    return LocalStore(tmp_path / "lake")


@pytest.fixture
def raw_record() -> dict[str, str | dict[str, str]]:
    """Un registro crudo de Socrata, tal cual llega de la API (todo string)."""
    return {
        "id_contrato": "CO1.PCCNTR.1",
        "nit_entidad": "899999999",
        "documento_proveedor": "1030602278",
        "nombre_entidad": "ALCALDIA DE PRUEBA",
        "proveedor_adjudicado": "Persona De Prueba",
        "departamento": "Distrito Capital de Bogotá",
        "estado_contrato": "Activo",
        "modalidad_de_contratacion": "Contratación directa",
        "fecha_de_firma": "2024-05-21T00:00:00.000",
        "fecha_de_inicio_del_contrato": "2024-06-01T00:00:00.000",
        "fecha_de_fin_del_contrato": "2025-01-31T00:00:00.000",
        "ultima_actualizacion": "2024-12-10T00:00:00.000",
        "valor_del_contrato": "39720000",
        "valor_pagado": "0",
        "es_pyme": "Si",
        "es_grupo": "No",
        "tipo_de_documento_supervisor": "Cédula de Ciudadanía",
        "urlproceso": {"url": "https://example.test/x"},
    }
