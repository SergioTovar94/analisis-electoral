"""
Aquí se encuentra el módulo principal de la conversión de datos a Parquet.
Este archivo contiene la función to_parquet que se encarga de convertir los
datasets en formato CSV o excel a Parquet, optimizando el almacenamiento y 
la velocidad de lectura para futuros análisis.
"""

import polars as pl


def to_parquet(path, name):
    """
    Función principal para la conversión de datos a Parquet. 
    Recibe un dataset en formato CSV o excel, lo convierte a Parquet 
    y lo guarda en la carpeta de outputs.
    """
    # Conversión en streaming
    (
    pl.scan_csv("datos_gigantes.csv")
    .sink_parquet("datos_gigantes.parquet")
    )
