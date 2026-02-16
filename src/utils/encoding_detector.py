"""
Módulo para detección de encoding de archivos.
Utiliza la biblioteca chardet para analizar una muestra de bytes del archivo
y determinar el encoding más probable, junto con un nivel de confianza.
Esto es útil para manejar archivos con encodings desconocidos o mixtos,
especialmente en contextos de análisis de datos donde la calidad de los archivos puede variar.    
"""
from pathlib import Path
from typing import List, Dict, Union
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
