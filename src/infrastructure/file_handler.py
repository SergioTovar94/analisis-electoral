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
        file_path: Union[str, Path],
        encoding: str = "utf8"
        ) -> pl.LazyFrame:
    """Lee un CSV en modo Lazy (apto para archivos grandes)."""
    if encoding.lower() == "utf8":
        return pl.scan_csv(file_path)
    df = pl.read_csv(file_path, encoding=encoding)
    return df.lazy()

def read_excel_lazy(file_path: Union[str, Path]) -> pl.LazyFrame:
    """
    Lee un Excel y devuelve un LazyFrame.
    NOTA: pl.read_excel es eager (carga en memoria), pero podemos convertirlo a lazy.
    Para archivos muy grandes (>1M filas) considera convertir el Excel a CSV primero.
    """
    # Carga eager, pero luego lazy
    df = pl.read_excel(file_path)
    return df.lazy()

def write_parquet(lf: pl.LazyFrame, output_path: Union[str, Path]) -> None:
    """
    Escribe un LazyFrame a Parquet usando sink_parquet (escritura directa sin collect).
    Ideal para grandes volúmenes.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(output_path)
    