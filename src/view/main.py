"""
Muestra el flujo principal del proyecto, integrando EDA, transformaciones y consultas. 
Este archivo es el punto de entrada para ejecutar todo el proceso de análisis de datos.
"""

from src.ingestion.main import ingestion_main
from src.eda.main import eda_main
from src.transformation.main import transformations_main

def main():
    """
    Función principal que muestra el menú de opciones para ejecutar EDA, 
    transformaciones o consultas.F
    """


    menu = {
        "1": "Preparación de datasets",
        "2": "Análisis Exploratorio de Datos (EDA)",
        "3": "Transformaciones",
        "4": "Consultas",
    }

    while True:
        print("\nMenú de opciones:")
        for key, value in menu.items():
            print(f"{key}. {value}")
        option = input("Seleccione una opción (1-3) o 'q' para salir: ")
        if option == "q":
            break
        elif option not in menu:
            print("\033[H\033[J", end="")
            print("x"*10,"Opción inválida. Intente de nuevo.", "x"*10)
        else:
            print("\033[H\033[J", end="")
            print("="*10, f"{menu[option]}","="*10)
            if option == "1":
                ingestion_main()
            elif option == "2":
                eda_main()
            elif option == "3":
                transformations_main()
            elif option == "4":
                print("Consultas aún no implementadas.")
