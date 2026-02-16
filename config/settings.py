from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_OUTPUTS = PROJECT_ROOT / "data" / "curated"

# Diccionario de datasets raw
RAW_DATASETS = {
    "ventas": DATA_RAW / "ventas.csv",
    "clientes": DATA_RAW / "clientes.csv",
    "productos": DATA_RAW / "productos.csv",
}

# Parámetros de transformación
DROP_COLUMNS = {...}  # config específica