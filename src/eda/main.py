"""
Módulo principal de la exploración de datos
"""

from config.settings import DATA_SILVER
from .profile import generate_profile


def eda_main():
    """
    Función principal para ejecutar el análisis exploratorio de datos (EDA).
    """
    for dataset in DATA_SILVER.glob("*.parquet"):
        print(f"Procesando dataset: {dataset.name}")
        generate_profile(dataset)
