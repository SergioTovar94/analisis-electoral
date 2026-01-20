import polars as pl
from sqlalchemy import create_engine

DB_URL = "postgresql+psycopg2://user:pass@localhost:5432/analisis_electoral"

def aggregate_by_puesto(df: pl.DataFrame):
    agg = df.groupby(["puesto", "municipio", "candidato"]).agg([
        pl.sum("votos").alias("votos_totales"),
        pl.sum("votos_nulos").alias("votos_nulos"),
        pl.sum("votos_blancos").alias("votos_blancos")
    ])
    return agg

def save_to_postgres(df: pl.DataFrame):
    engine = create_engine(DB_URL)
    df_pandas = df.to_pandas()
    df_pandas.to_sql("resultado_puesto", engine, if_exists="replace", index=False)

if __name__ == "__main__":
    import clean_data
    df_clean = clean_data.clean_data(clean_data.load_raw.load_files("data/"))
    df_agg = aggregate_by_puesto(df_clean)
    save_to_postgres(df_agg)
    print("Agregación finalizada y guardada en PostgreSQL")
