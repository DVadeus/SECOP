"""App CLI de SECOP (Typer).

Comandos:

* ``secop etl backfill``     — carga histórica (local, reanudable)
* ``secop etl incremental``  — actualización incremental (la usa GitHub Actions)
* ``secop etl reparar``      — corrige datos escritos por versiones anteriores
* ``secop query``            — consulta el lake por filtros y exporta a Excel/CSV
* ``secop sql``              — SQL DuckDB crudo sobre las vistas de todas las bases
* ``secop historial``        — versiones reemplazadas de contratos, por NIT
* ``secop info``             — estado del lake (particiones, filas, watermark)

Los comandos de ETL y de consulta aceptan ``--base contratos|interadministrativos``
(por defecto ``contratos``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from secop.config import DEFAULT_START_YEAR
from secop.logsetup import setup_logging

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="ETL y consultas del data lake SECOP II.",
)
etl_app = typer.Typer(no_args_is_help=True, help="Ejecución del ETL.")
app.add_typer(etl_app, name="etl")

console = Console()
logger = logging.getLogger("secop")


@app.callback()
def _main(
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Logging en nivel DEBUG.")
    ] = False,
) -> None:
    setup_logging(logging.DEBUG if verbose else logging.INFO)


# --------------------------------------------------------------------------- #
# ETL                                                                         #
# --------------------------------------------------------------------------- #
def _store(target: str):
    from secop.storage import get_store

    return get_store(target)


def _base(name: str):
    from secop.bases import get_base

    try:
        return get_base(name)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--base") from exc


@etl_app.command("backfill")
def etl_backfill(
    start: Annotated[
        int, typer.Option(help="Año inicial (inclusive).")
    ] = DEFAULT_START_YEAR,
    end: Annotated[int, typer.Option(help="Año final (inclusive).")] = datetime.now(
        UTC
    ).year,
    target: Annotated[str, typer.Option(help="Destino: 'r2' o 'local'.")] = "r2",
    year: Annotated[
        int | None, typer.Option(help="Re-procesar sólo este año (ignora start/end).")
    ] = None,
    force: Annotated[
        bool, typer.Option(help="Rehacer años que ya existan en el lake.")
    ] = False,
    base: Annotated[
        str, typer.Option(help="Base: 'contratos' o 'interadministrativos'.")
    ] = "contratos",
) -> None:
    """Carga histórica completa. Pensada para correr en local, una vez.

    Una base que se baja completa (interadministrativos) ignora los años.
    """
    from secop.pipeline import backfill

    result = backfill(
        _store(target),
        start_year=start,
        end_year=end,
        force=force,
        only_year=year,
        base=_base(base),
    )
    _print_run_result(result)


@etl_app.command("incremental")
def etl_incremental(
    target: Annotated[str, typer.Option(help="Destino: 'r2' o 'local'.")] = "r2",
    base: Annotated[
        str, typer.Option(help="Base: 'contratos' o 'interadministrativos'.")
    ] = "contratos",
) -> None:
    """Compara el origen con el lake y descarga lo nuevo o cambiado. No borra nada."""
    from secop.pipeline import incremental

    result = incremental(_store(target), _base(base))
    _print_run_result(result)


@etl_app.command("reparar")
def etl_reparar(
    aplicar: Annotated[
        bool,
        typer.Option(help="Aplicar los cambios (por defecto sólo se informa)."),
    ] = False,
    target: Annotated[str, typer.Option(help="Destino: 'r2' o 'local'.")] = "r2",
    base: Annotated[
        str, typer.Option(help="Base: 'contratos' o 'interadministrativos'.")
    ] = "contratos",
) -> None:
    """Saca de year=unknown los contratos que ya están en un año real.

    Corrige lo escrito por versiones anteriores del ETL. Respalda antes en
    backup/<fecha>/ y las copias movidas quedan en el historial.
    """
    from secop.pipeline import repair

    report = repair(_store(target), apply=aplicar, base=_base(base))
    console.print(
        f"Contratos en year=unknown que ya tienen año: "
        f"[bold]{report.unknown_with_date:,}[/bold]"
    )
    console.print(
        f"Contratos en dos o más años (evidencia, no se tocan): "
        f"[bold]{report.year_changes:,}[/bold]"
    )
    if not report.applied:
        console.print("[yellow]Sólo informe. Usa --aplicar para corregir.[/yellow]")
        return
    if report.backup_prefix:
        console.print(f"Respaldo en: {report.backup_prefix}/")
    _print_run_result(report.result)


def _print_run_result(result: object) -> None:
    from secop.pipeline import RunResult

    assert isinstance(result, RunResult)
    table = Table(title=f"Corrida: {result.mode} — particiones reescritas")
    table.add_column("Partición")
    table.add_column("Filas totales", justify="right")
    for partition, count in sorted(result.partitions.items()):
        table.add_row(partition, f"{count:,}")
    if result.skipped:
        table.add_row("(omitidas)", ", ".join(result.skipped))
    console.print(table)
    console.print(
        "Contratos escritos (nuevos o actualizados): "
        f"[bold]{result.contracts_written:,}[/bold]"
    )
    console.print(f"Watermark: [bold]{_watermark_text(result.new_watermark)}[/bold]")


def _watermark_text(value: str | None) -> str:
    return value or "no aplica (la base no tiene columna de versión)"


# --------------------------------------------------------------------------- #
# Consulta                                                                    #
# --------------------------------------------------------------------------- #
_OptList = Annotated[list[str] | None, typer.Option()]


@app.command("query")
def query_cmd(
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Archivo de salida (.xlsx/.csv/.parquet)."),
    ],
    nit_proveedor: Annotated[
        list[str] | None,
        typer.Option("--nit-proveedor", help="NIT del contratista (repetible)."),
    ] = None,
    nit_entidad: Annotated[
        list[str] | None,
        typer.Option(
            "--nit-entidad", help="NIT de la entidad contratante (repetible)."
        ),
    ] = None,
    entidad_contains: Annotated[
        str | None, typer.Option(help="Texto contenido en nombre_entidad.")
    ] = None,
    proveedor_contains: Annotated[
        str | None,
        typer.Option(help="Texto en proveedor_adjudicado (o 'contratista')."),
    ] = None,
    departamento: _OptList = None,
    estado: _OptList = None,
    modalidad: _OptList = None,
    plataforma: Annotated[
        list[str] | None,
        typer.Option(help="SECOP_I o SECOP_II (sólo interadministrativos)."),
    ] = None,
    year_from: Annotated[int | None, typer.Option(help="Año de inicio desde.")] = None,
    year_to: Annotated[int | None, typer.Option(help="Año de inicio hasta.")] = None,
    valor_min: Annotated[
        float | None, typer.Option(help="valor_del_contrato mínimo.")
    ] = None,
    valor_max: Annotated[
        float | None, typer.Option(help="valor_del_contrato máximo.")
    ] = None,
    columns: Annotated[
        str | None, typer.Option(help="Columnas a exportar, separadas por coma.")
    ] = None,
    limit: Annotated[int | None, typer.Option(help="Máximo de filas.")] = None,
    source: Annotated[str, typer.Option(help="Origen: 'r2' o 'local'.")] = "r2",
    path: Annotated[
        Path | None,
        typer.Option(help="Directorio Parquet a consultar (con --source local)."),
    ] = None,
    base: Annotated[
        str, typer.Option(help="Base: 'contratos' o 'interadministrativos'.")
    ] = "contratos",
) -> None:
    """Filtra una base del lake por características y exporta el resultado."""
    from secop.query import QueryEngine, export_dataframe
    from secop.query.filters import QuerySpec, build_query

    spec = QuerySpec(
        nit_proveedor=list(nit_proveedor or []),
        nit_entidad=list(nit_entidad or []),
        entidad_contains=entidad_contains,
        proveedor_contains=proveedor_contains,
        departamento=list(departamento or []),
        estado=list(estado or []),
        modalidad=list(modalidad or []),
        plataforma=list(plataforma or []),
        year_from=year_from,
        year_to=year_to,
        valor_min=valor_min,
        valor_max=valor_max,
        columns=[c.strip() for c in columns.split(",")] if columns else [],
        limit=limit,
    )
    chosen = _base(base)
    sql, params = _build(build_query, spec, chosen)
    logger.debug("SQL generado:\n%s", sql)
    if spec.nit_entidad and chosen.column_map:
        console.print(
            "[yellow]--nit-entidad en interadministrativos se resuelve con el "
            "código de entidad de la base de contratos: sólo encuentra convenios "
            "de SECOP II. Para SECOP I usa --entidad-contains.[/yellow]"
        )

    with QueryEngine.from_source(source, path=path) as engine:
        _require(engine, chosen.name)
        if spec.nit_entidad and chosen.column_map:
            _require(engine, "contratos")  # el NIT se resuelve con contratos
        df = engine.run(sql, params)

    if df.empty:
        console.print("[yellow]0 filas. No se escribió ningún archivo.[/yellow]")
        raise typer.Exit(code=1)

    written = export_dataframe(df, output)
    console.print(f"[green]{len(df):,} filas[/green] -> {written}")


def _build(
    builder: Callable[..., tuple[str, list[object]]], *args: object
) -> tuple[str, list[object]]:
    """Ejecuta ``builder`` mostrando los filtros no soportados como error de uso."""
    from secop.query.filters import UnsupportedFilterError

    try:
        return builder(*args)
    except UnsupportedFilterError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _require(engine: object, view: str) -> None:
    from secop.query import QueryEngine

    assert isinstance(engine, QueryEngine)
    try:
        engine.require_view(view)
    except LookupError as exc:
        console.print(f"[yellow]{exc}.[/yellow]")
        raise typer.Exit(code=1) from exc


@app.command("sql")
def sql_cmd(
    query: Annotated[
        str,
        typer.Argument(
            help="SQL DuckDB. Vistas: contratos, interadministrativos y sus "
            "*_historial."
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Si se indica, exporta el resultado."),
    ] = None,
    source: Annotated[str, typer.Option(help="Origen: 'r2' o 'local'.")] = "r2",
    path: Annotated[
        Path | None, typer.Option(help="Directorio Parquet (con --source local).")
    ] = None,
    limit_preview: Annotated[
        int, typer.Option(help="Filas a mostrar en consola si no hay --output.")
    ] = 50,
) -> None:
    """Ejecuta SQL crudo sobre las vistas del lake (todas las bases)."""
    from secop.query import QueryEngine, export_dataframe

    with QueryEngine.from_source(source, path=path) as engine:
        df = engine.run(query)

    if output is not None:
        written = export_dataframe(df, output)
        console.print(f"[green]{len(df):,} filas[/green] -> {written}")
        return

    _print_dataframe(df.head(limit_preview))
    if len(df) > limit_preview:
        console.print(
            f"... {len(df) - limit_preview:,} filas más (usa -o para exportar)"
        )


@app.command("historial")
def historial_cmd(
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Archivo de salida (.xlsx/.csv/.parquet)."),
    ],
    nit_proveedor: Annotated[
        list[str] | None,
        typer.Option("--nit-proveedor", help="NIT del contratista (repetible)."),
    ] = None,
    nit_entidad: Annotated[
        list[str] | None,
        typer.Option(
            "--nit-entidad", help="NIT de la entidad contratante (repetible)."
        ),
    ] = None,
    source: Annotated[str, typer.Option(help="Origen: 'r2' o 'local'.")] = "r2",
    path: Annotated[
        Path | None, typer.Option(help="Directorio Parquet (con --source local).")
    ] = None,
    base: Annotated[
        str, typer.Option(help="Base: 'contratos' o 'interadministrativos'.")
    ] = "contratos",
) -> None:
    """Exporta las versiones anteriores (reemplazadas) de los contratos de un NIT.

    Sale del lake de historial, aparte de ``secop query``, para que la consulta
    principal nunca devuelva duplicados.
    """
    from secop.query import QueryEngine, export_dataframe
    from secop.query.filters import build_history_query

    chosen = _base(base)
    try:
        sql, params = build_history_query(
            list(nit_proveedor or []), list(nit_entidad or []), chosen
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    with QueryEngine.from_source(source, path=path) as engine:
        if not engine.has_view(chosen.history_view):
            console.print("[yellow]Aún no hay historial de versiones.[/yellow]")
            raise typer.Exit(code=1)
        if nit_entidad and chosen.column_map:
            _require(engine, "contratos")  # el NIT se resuelve con contratos
        df = engine.run(sql, params)

    if df.empty:
        console.print("[yellow]0 versiones. No se escribió ningún archivo.[/yellow]")
        raise typer.Exit(code=1)

    written = export_dataframe(df, output)
    console.print(f"[green]{len(df):,} versiones[/green] -> {written}")


def _print_dataframe(df: object) -> None:
    import pandas as pd

    assert isinstance(df, pd.DataFrame)
    table = Table(show_lines=False)
    for col in df.columns:
        table.add_column(str(col))
    for _, row in df.iterrows():
        table.add_row(*(str(v) for v in row.tolist()))
    console.print(table)


# --------------------------------------------------------------------------- #
# Info                                                                        #
# --------------------------------------------------------------------------- #
@app.command("info")
def info_cmd(
    source: Annotated[str, typer.Option(help="Origen: 'r2' o 'local'.")] = "r2",
    path: Annotated[
        Path | None, typer.Option(help="Directorio Parquet (con --source local).")
    ] = None,
    base: Annotated[
        str, typer.Option(help="Base: 'contratos' o 'interadministrativos'.")
    ] = "contratos",
) -> None:
    """Muestra el estado de una base: particiones, filas, fechas, watermark."""
    from secop.query import QueryEngine

    chosen = _base(base)
    with QueryEngine.from_source(source, path=path) as engine:
        _require(engine, chosen.name)
        summary = engine.partition_summary(chosen)
        total = int(summary["filas"].sum()) if not summary.empty else 0
        lo, hi = engine.date_range(chosen)
        glob = engine.globs[chosen.name]

    table = Table(title=f"Lake SECOP: {chosen.name} ({source})")
    table.add_column("year")
    table.add_column("filas", justify="right")
    for _, r in summary.iterrows():
        table.add_row(str(r["year"]), f"{int(r['filas']):,}")
    table.add_row("[bold]TOTAL[/bold]", f"[bold]{total:,}[/bold]")
    console.print(table)
    console.print(f"Rango {chosen.partition_date_column}: {lo} -> {hi}")
    console.print(f"Glob: {glob}")

    if source == "r2":
        _print_state(chosen)


def _print_state(base: object) -> None:
    from secop.bases import Base
    from secop.state import load_state
    from secop.storage import get_store

    assert isinstance(base, Base)
    try:
        state = load_state(get_store("r2"), base)
    except Exception as exc:  # noqa: BLE001 - sólo informativo
        console.print(f"[yellow]No se pudo leer el estado: {exc}[/yellow]")
        return
    # Una base sin columna de versión no tiene watermark (aunque un estado viejo
    # guarde el valor por defecto).
    watermark = state.ultima_actualizacion if base.version_column else None
    console.print(
        f"Watermark ({base.version_column or 'sin columna de versión'}): "
        f"[bold]{_watermark_text(watermark)}[/bold]"
    )
    console.print(f"Última corrida: {state.last_run_utc}")
    console.print(f"Versión de esquema: {state.schema_version}")


if __name__ == "__main__":
    app()
