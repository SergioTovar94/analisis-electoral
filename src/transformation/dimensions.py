"""
En este módulo se crean las dimensaiones gold para ser cargadas a la base de datos. 
Estas dimensiones se crean a partir de las tablas silver y se transforman 
para cumplir con los requisitos de la base de datos.
"""
import polars as pl
from config.settings import DATA_RAW, DATA_TRANSFORMED

def create_dim_municipio() -> pl.DataFrame:
    """
    Crea la dimensión de municipio
    """
    df_municipio = pl.read_csv(DATA_RAW / "DIVIPOLA.csv", sep=";")
    dim_municipio = (
        df_municipio.select([
            pl.col("DEP").alias("cod_departamento"),
            pl.col("DEPARTAMENTO").alias("nombre_departamento"),
            pl.col("MUN").alias("cod_municipio"),
            pl.col("MUNICIPIO").alias("nombre_municipio"),
        ])
        .unique()
        .with_row_index("id_municipio")
    )
    dim_municipio = dim_municipio.with_columns([
        pl.col("cod_departamento").cast(pl.Utf8),
        pl.col("cod_municipio").cast(pl.Utf8)
    ])
    dim_municipio.write_parquet(DATA_TRANSFORMED / "dim_municipio.parquet")
    return dim_municipio

def create_dim_puesto(dim_municipio: pl.DataFrame) -> pl.DataFrame:
    """
    Crea la dimensión de puestos
    """
    df_puesto = pl.read_csv(DATA_RAW / "DIVIPOLA.csv", sep=";")
    dim_puesto = (
        df_puesto.select([
            pl.col("PSTO").alias("cod_puesto"),
            pl.col("PUESTO").alias("nombre_puesto"),
            pl.col("ZONA").alias("zona"),
            pl.col("MUN").alias("cod_municipio"),
        ])
        .unique()
    )
    dim_puesto = (
        dim_puesto.join(
            dim_municipio.select(["id_municipio", "cod_municipio"]),
            on="cod_municipio",
            how="left"
        )
        .drop("cod_municipio")
        .with_row_index("id_puesto")
    )
    dim_puesto = dim_puesto.with_columns([
        pl.col("cod_puesto").cast(pl.Utf8),
        pl.col("zona").cast(pl.Utf8)
    ])
    dim_puesto.write_parquet(DATA_TRANSFORMED / "dim_puesto.parquet")
    return dim_puesto

def create_dim_eleccion() -> pl.DataFrame:
    """
    Crea la dimensión de elecciones
    """
    df_eleccion = pl.read_csv(DATA_RAW / "ELECCIONES.CSV", sep=";")
    dim_eleccion = (
        df_eleccion.select([
            pl.col("ANIO_ELECCION").alias("anio_eleccion"),
            pl.col("TIPO_ELECCION").alias("tipo_eleccion")
        ])
        .unique()
        .with_row_index("id_eleccion")
    ) 
    dim_eleccion.write_parquet(DATA_TRANSFORMED / "dim_eleccion.parquet")
    return dim_eleccion
