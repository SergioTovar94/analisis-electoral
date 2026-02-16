"""
Aquí se alojan todas las funciones utilidades que no encajan en las otras categorías,
pero que son necesarias para el correcto funcionamiento del pipeline.
"""

from config.settings import RAW_DATASETS, DATA_UTILS
from .encoding_detector import detect_encoding, save_encoding_report

def utils_main():
    """
    Función principal para ejecutar las utilidades del proyecto. 
    Actualmente no hay utilidades implementadas, pero esta función sirve como punto de entrada
    para futuras herramientas que puedan ser necesarias.
    """
    utilidades = {
        "1": "Detección de encoding de archivos",
        "2": "Pasar a csv utf8"
    }

    while True:
        print("\nSeleccione una utilidad:")
        for key, value in utilidades.items():
            print(f"{key}. {value}")
        opcion = input("Ingrese el número de la utilidad deseada: ")
        if opcion == "1":
            results = []
            for _, meta in RAW_DATASETS.items():
                path = meta["path"]
                encoding = detect_encoding(path)
                results.append(encoding)
            save_encoding_report(results, DATA_UTILS / "encoding_report.csv")
            break
        elif opcion == "2":
            print("Funcionalidad en desarrollo. Por favor, revise el repositorio para futuras actualizaciones.")
            break
        else:
            print("Opción no válida. Por favor, intente nuevamente.")
