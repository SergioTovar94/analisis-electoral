"""
Módulo para detección de encoding de archivos.
Utiliza la biblioteca chardet para analizar una muestra de bytes del archivo
y determinar el encoding más probable, junto con un nivel de confianza.
Esto es útil para manejar archivos con encodings desconocidos o mixtos,
especialmente en contextos de análisis de datos donde la calidad de los archivos puede variar.    
"""
from pathlib import Path
from typing import List, Dict, Union
import polars as pl
import csv
import chardet

def detect_encoding(
    file_path: Union[str, Path],
    sample_size: int = 100_000
) -> Dict[str, str]:
    """
    Detecta el encoding probable de un archivo leyendo una muestra de bytes.

    Parameters
    ----------
    file_path : str | Path
        Ruta del archivo.
    sample_size : int
        Cantidad de bytes a leer para detección.

    Returns
    -------
    dict
        Diccionario con encoding detectado y nivel de confianza.
        Ejemplo:
        {
            "encoding": "ISO-8859-1",
            "confidence": 0.73
        }
    """
    file_path = Path(file_path)

    with file_path.open("rb") as f:
        raw_data = f.read(sample_size)
    result = chardet.detect(raw_data)

    return {
        "file": file_path.name,
        "encoding": result.get("encoding"),
        "confidence": result.get("confidence"),
    }

def save_encoding_report(
    results: List[Dict],
    output_path: Path
) -> None:
    """
    Guarda un reporte de encodings detectados en un archivo CSV.

    :param results: Lista de diccionarios con los resultados de detección de encoding.
    :type results: List[Dict]
    :param output_path: Ruta del archivo CSV donde se guardará el reporte.
    :type output_path: Path
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["file", "encoding", "confidence"]
        )
        writer.writeheader()
        writer.writerows(results)

def convert_to_utf8(
    input_path: Union[str, Path],
    source_encoding: str,
    output_path: Union[str, Path] | None = None,
    chunk_size: int = 1024 * 1024  # 1 MB
) -> Path:
    """
    Convierte un archivo grande a UTF-8 sin cargarlo completo en memoria.
    """
    input_path = Path(input_path)

    if output_path is None:
        output_path = input_path.with_suffix(".utf8.csv")
    else:
        output_path = Path(output_path)

    with open(input_path, "r", encoding=source_encoding, errors="strict") as f_in, \
         open(output_path, "w", encoding="utf8") as f_out:

        while True:
            chunk = f_in.read(chunk_size)
            if not chunk:
                break
            f_out.write(chunk)

    return output_path

def normalize_and_store(meta, silver_dir):
    """Toma un archivo CSV, lo normaliza a UTF-8 si es necesario y lo guarda en formato Parquet."""
    input_path = Path(meta["path"])
    encoding = meta.get("encoding", "utf8")

    if encoding.lower() != "utf8":
        input_path = convert_to_utf8(input_path, encoding)
    (
        pl.scan_csv(input_path)
        .sink_parquet(silver_dir)
    )
