"""En este módulo se seleccionan las variables relevantes para cada dataset
"""
from pathlib import Path
import json
import polars as pl
from config.settings import DATA_SILVER, COLUMN_MAPPING

def mapear_variables():
    """
    Importa el diccionario con los nombres de datasets, pregunta al 
    usuario por cada dataset si será utilizado luego tomará el primer
    dataset y preguntará por la variable a seleccionar, pasará recursivamente 
    al siguiente dataset preguntando si la variable está. Así hasta seleccionar
    todas las variables, luego se pedirá el nombre a asignar y quedará en todos los datasets
    """
    datasets = {}
    for dataset in DATA_SILVER.glob("*.parquet"):
        ruta = Path(dataset)
        respuesta = input(f"¿Desea utilizar el dataset {ruta.stem}? (s/n): ")
        if respuesta.lower() == 's':
            schema = pl.read_parquet_schema(f"{dataset}")
            datasets[ruta.stem] = list(schema.keys())
    standard_schema = build_standard_schema(datasets)
    with open(COLUMN_MAPPING, "w", encoding="utf-8") as f:
        json.dump(standard_schema, f, indent=4)


def build_standard_schema(datasets: dict) -> dict:
    """
    Construye un esquema estándar de nombres de columnas a partir de los datasets seleccionados.
    """
    print(datasets)
    dataset_names = list(datasets.keys())
    base_dataset = dataset_names[0]

    standard_schema = {}

    print(f"\nDataset base: {base_dataset}\n")

    for base_column in datasets[base_dataset]:
        print(f"\nColumna base: {base_column}")
        use = input("¿Deseas estandarizar esta columna? (s/n): ").strip().lower()
        if use != "s":
            continue

        standard_name = input("Nombre estándar a asignar: ").strip()

        standard_schema[standard_name] = {
            base_dataset: base_column
        }
        for dataset in dataset_names[1:]:
            print(f"\nDataset: {dataset}")
            columnas = {}
            i = 1
            print("Columnas disponibles:")
            for i, col in enumerate(datasets[dataset], 1):
                columnas[i] = col
                print(f"{i}. {col}")
                i += 1

            match = input(
                f"¿Qué columna corresponde a '{standard_name}' en este dataset? "
                "(Enter si no existe): "
            ).strip()

            if match:
                standard_schema[standard_name][dataset] = columnas.get(int(match))

    return standard_schema