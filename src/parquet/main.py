"""
Módulo principal de la conversión de datos a Parquet. 
Este archivo contiene la función `parquet_main` que se encarga de 
convertir los datasets en formato CSV o excel a Parquet, optimizando el almacenamiento y la 
velocidad de lectura para futuros análisis.
"""

from config.settings import RAW_DATASETS, DATA_SILVER
from src.infrastructure.file_handler import read_csv_lazy, write_parquet, read_excel_lazy

def parquet_main():
    """
    Función principal para la conversión de datos a Parquet. 
    Lee los datasets en formato CSV o excel, los convierte a Parquet y 
    los guarda en la carpeta de outputs.
    """
    opciones = {
        "1": "Convertir todos los archivos a Parquet",
    }

    while True:
        print("\nSeleccione una opción:")
        for key, value in opciones.items():
            print(f"{key}. {value}")
        opcion = input("Ingrese el número de la opción deseada: ")
        if opcion == "1":
            # Lógica para convertir todos los archivos a Parquet
            print("Convirtiendo todos los archivos a Parquet...")
            for name, meta in RAW_DATASETS.items():
                path = meta["path"]
                file_type = meta["type"]
                if file_type == "csv":
                    df = read_csv_lazy(path, encoding=meta.get("encoding", "utf8"))
                elif file_type == "excel":
                    df = read_excel_lazy(path)
                else:
                    raise ValueError(f"Unsupported file type: {file_type}")
                output_path = DATA_SILVER / f"{name}.parquet"
                write_parquet(df, output_path)
            break
        else:
            print("Opción no válida. Por favor, intente nuevamente.")
