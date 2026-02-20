"""
Este módulo se encarga de generar perfiles de datos utilizando la biblioteca ydata_profiling.
"""
import polars as pl
from config.settings import DATA_EDA

def generate_profile(dataset, sample_size: int = 5):
    """
    Genera un perfil de datos para un dataset dado.
    
    :param dataset_name: Nombre del dataset
    :param df: DataFrame de Polars con los datos del dataset
    """
    texto = ""
    lf = pl.scan_parquet(dataset)
    n_rows = lf.select(pl.len()).collect().item()
    schema = lf.schema
    n_cols = len(schema)
    texto += f"📊 Dataset: {dataset.name}\n"
    texto += f"📊 Filas: {n_rows}\n"
    texto += f"📦 Columnas: {n_cols}\n"
    texto += "📑 Esquema:\n"
    for col, dtype in schema.items():
        texto += f"  - {col}: {dtype}\n"

    # Tomar muestra aleatoria pequeña
    df_sample = (
        lf
        .select(pl.all())
        .limit(100_000)  # limitamos lectura para no cargar 3GB
        .collect()
        .sample(n=sample_size, seed=42)
    )

    texto += "\n🔎 Muestra aleatoria (5 valores por columna):\n"
    tabla = "| Columna | Tipo |Tipo | Tipo | Tipo | Tipo \n|---------|------|------|------|------|------|\n"
    for col in df_sample.columns:
        print(repr(col))
    for col in df_sample.columns:
        col_clean = clean_cell(col)
        values = df_sample[col].to_list()
        values_str = [clean_cell(v) for v in values]
        fila = f"|{col_clean}|" + "|".join(values_str) + "|\n"
        tabla += fila

    # Guardar el perfil en un archivo de texto
    with open(DATA_EDA / f"profile_{dataset.stem}.md", "w", encoding="utf-8") as f:
        f.write(texto)
        f.write("\n")
        f.write(tabla)

def clean_cell(value):
    """Limpia el valor de una celda para su presentación en la tabla."""
    if value is None:
        return ""
    return (
        str(value)
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("|", "/")
        .strip()
    )
