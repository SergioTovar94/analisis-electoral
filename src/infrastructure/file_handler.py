"""
Módulo para manejo de archivos con Polars.
Este módulo contiene funciones para leer archivos CSV y Excel en modo Lazy,
así como para escribir DataFrames a formato Parquet, 
optimizando el manejo de grandes volúmenes de datos.
"""
from pathlib import Path
from typing import Union
import polars as pl

def read_csv_lazy(
        file_path: Union[str, Path]) -> pl.LazyFrame:
    """Lee un CSV en modo Lazy (apto para archivos grandes)."""
    return pl.scan_csv(file_path)

def write_parquet(lf: pl.LazyFrame, output_path: Union[str, Path]) -> None:
    """
    Escribe un LazyFrame a Parquet usando sink_parquet (escritura directa sin collect).
    Ideal para grandes volúmenes.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(output_path)
    