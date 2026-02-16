"""
Archivo de configuración para el proyecto de análisis electoral.
Este archivo define las rutas de los datasets, así como parámetros de transformación y limpieza.
Aquí se centralizan las configuraciones para facilitar su mantenimiento y actualización.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_SILVER = PROJECT_ROOT / "data" / "silver"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_OUTPUTS = PROJECT_ROOT / "data" / "curated"
DATA_UTILS = PROJECT_ROOT / "data" / "utils"

# Diccionario de datasets raw
RAW_DATASETS = {
    "senado_2018": {
        "path" : DATA_RAW / "2018" / "2018_SENADO.csv",
        "type": "csv",
        "encoding": "utf16"
        },
    "presidencia_2018_1v": {
        "path" : DATA_RAW / "2018" / "MMV_NACIONAL_PRESIDENTE_2018_1v.xlsx",
        "type": "xlsx"
        },
    "camara_2022": {
        "path" : DATA_RAW / "2022" / "2022_CAMARA.csv",
        "type": "csv",
        "encoding": "utf8"
        },
    "centro_esperanza_2022": {
        "path" : DATA_RAW / "2022" / "2022_CONSULTA_CENTRO_ESPERANZA.csv",
        "type": "csv",
        "encoding": "utf16"
        },
    "senado_2022": {
        "path" : DATA_RAW / "2022" / "2022_SENADO.csv",
        "type": "csv",
        "encoding": "utf16"
        },
    "territoriales_2023": {
        "path" : DATA_RAW / "2023" / "MMV_2023_15_CUNDINAMARCA.csv",
        "type": "csv",
        "encoding": "utf8"
        },
    }

# Parámetros de transformación
DROP_COLUMNS = {...}  # config específica
