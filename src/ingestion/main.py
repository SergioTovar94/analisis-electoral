"""
Módulo principal del alistamiento de los datasets. 
Este archivo contiene la función `ingestion_main` que permite identificar codificación, 
transformarla y convertir los datasets en formato CSV a Parquet, optimizando el almacenamiento y la 
velocidad de lectura para futuros análisis.
"""

from config.settings import RAW_DATASETS, DATA_SILVER
from .encoding import detect_encoding, save_encoding_report, normalize_and_store

def ingestion_main():
    """
    Función principal para la conversión de datos a Parquet. 
    Lee los datasets en formato CSV o excel, los convierte a Parquet y 
    los guarda en la carpeta de outputs.
    """
    opciones = {
        "1": "Identificar codificación",
        "2": "Convertir todos los archivos a Parquet",
    }

    while True:
        print("\nSeleccione una opción:")
        for key, value in opciones.items():
            print(f"{key}. {value}")
        opcion = input("Ingrese el número de la opción deseada: ")
        if opcion == "1":
            print("Identificando codificación de los archivos...")
            results = []
            for _, meta in RAW_DATASETS.items():
                path = meta["path"]
                results.append(detect_encoding(path))
            save_encoding_report(results, DATA_SILVER / "encoding_report.csv")
        elif opcion == "2":
            print("Transformando archivos a Parquet...")
            for _, meta in RAW_DATASETS.items():
                try:
                    path = meta["path"]
                    output_path = DATA_SILVER / f"{path.stem}.parquet"
                    normalize_and_store(meta, output_path)
                except Exception as e:
                    raise RuntimeError(
                        f"Error procesando archivo: {meta['file_name']}"
                    ) from e
            break
        else:
            print("Opción no válida. Por favor, intente nuevamente.")
