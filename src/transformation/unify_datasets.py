"""
Unificar datasets de candidatos en un solo archivo parquet
"""
import json
import polars as pl
from config.settings import DATA_TRANSFORMED, COLUMN_MAPPING, DATA_SILVER

def unify_datasets():
    """
    Unifica los datasets de candidatos en un solo archivo parquet.
    """

    with open(COLUMN_MAPPING, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    archivos = DATA_SILVER.glob("*.parquet")

    for archivo in archivos:
        nombre_archivo = archivo.stem
        df = pl.read_parquet(DATA_SILVER / archivo)

        # Construir diccionario de renombrado según el mapeo
        rename_dict = {}
        for col_std, archivos_map in mapping.items():
            if nombre_archivo in archivos_map:
                rename_dict[archivos_map[nombre_archivo]] = col_std.lower()  # lowercase para consistencia
        
        # Renombrar columnas
        df = df.rename(rename_dict)
    # Guardar
    df.write_parquet(DATA_TRANSFORMED / "candidatos_unificados.parquet")