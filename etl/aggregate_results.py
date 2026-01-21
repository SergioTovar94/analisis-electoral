import polars as pl
import psycopg2
from sqlalchemy import create_engine, text
from pathlib import Path

DB_URL = "postgresql://postgres:postgres@postgres:5432/analisis_electoral"
DATASET_FILTRADO = "data_processed/dataset_filtrado.csv"
TMP_CSV = "/tmp/resultado_puesto.csv"


def load_dataset_filtrado(path: str) -> pl.DataFrame:
    print(f"📥 Cargando dataset filtrado: {path}")
    return pl.read_csv(path)


def aggregate_by_puesto(df: pl.DataFrame) -> pl.DataFrame:
    """
    Agrega votos a nivel de puesto de votación.
    GOLD LAYER
    """

    required_cols = {
        "anio_eleccion",
        "cod_departamento",
        "departamento",
        "cod_municipio",
        "municipio",
        "cod_puesto",
        "puesto_votacion",
        "cod_candidato",
        "candidato",
        "votos",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"❌ Faltan columnas requeridas: {missing}")

    return (
        df
        .with_columns(pl.col("votos").cast(pl.Int64))
        .group_by(
            [
                "anio_eleccion",
                "cod_departamento",
                "departamento",
                "cod_municipio",
                "municipio",
                "cod_puesto",
                "puesto_votacion",
                "cod_candidato",
                "candidato",
            ]
        )
        .agg(pl.sum("votos").alias("votos_totales"))
        .sort(
            [
                "anio_eleccion",
                "departamento",
                "municipio",
                "puesto_votacion",
                "votos_totales",
            ],
            descending=[False, False, False, False, True],
        )
    )


def save_to_postgres_copy(df: pl.DataFrame, table_name: str = "resultado_puesto"):
    print("🧹 Escribiendo CSV temporal para COPY...")
    df.write_csv(TMP_CSV)

    print("🔌 Conectando a PostgreSQL...")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:
        print(f"🧹 TRUNCATE {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")

        print("🚀 Ejecutando COPY FROM STDIN...")
        with open(TMP_CSV, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"""
                COPY {table_name} (
                    anio_eleccion,
                    cod_departamento,
                    departamento,
                    cod_municipio,
                    municipio,
                    cod_puesto,
                    puesto_votacion,
                    cod_candidato,
                    candidato,
                    votos_totales
                )
                FROM STDIN WITH CSV HEADER
                """,
                f
            )

        conn.commit()
        print("✅ COPY finalizado correctamente")

    finally:
        cur.close()
        conn.close()
        Path(TMP_CSV).unlink(missing_ok=True)


def run():
    df = load_dataset_filtrado(DATASET_FILTRADO)

    print("📊 Agregando votos por puesto...")
    df_agg = aggregate_by_puesto(df)

    print("💾 Guardando en PostgreSQL (COPY)...")
    save_to_postgres_copy(df_agg)

    print("🏁 Proceso GOLD finalizado correctamente")


if __name__ == "__main__":
    run()
