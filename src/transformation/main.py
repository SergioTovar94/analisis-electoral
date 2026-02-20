""""
Módulo principal de la transformación de datos
"""

from .feature_mapping import mapear_variables
from .dimensions import create_dim_municipio
from .unify_datasets import unify_datasets

def transformations_main():
    """
    Función principal para ejecutar las transformaciones de datos.
    """
    opciones = {
        "1": "Mapeo de variables",
        "2": "Unificación de datasets",
        "3": "Creación de dimensiones",
    }

    while True:
        print("\nSeleccione una opción:")
        for key, value in opciones.items():
            print(f"{key}. {value}")
        opcion = input("Ingrese el número de la opción deseada: ")
        if opcion == "1":
            print("Mapeando variables...")
            mapear_variables()
        elif opcion == "2":
            print("Unificando datasets...")
            unify_datasets()
        elif opcion == "3":
            print("Creando dimensiones...")
            create_dim_municipio()
        elif opcion == "0":
            print("Saliendo del programa.")
            break
        else:
            print("Opción no válida. Por favor, intente de nuevo.")
