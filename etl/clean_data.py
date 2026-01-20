import polars as pl

def clean_data(df: pl.DataFrame):
    # Filtrar solo Cundinamarca
    df = df.filter(pl.col("departamento") == "Cundinamarca")
    
    # Normalizar nombres de columnas
    df = df.rename({
        "Puesto": "puesto",
        "Municipio": "municipio",
        "Votos": "votos",
        "Votos Nulos": "votos_nulos",
        "Votos Blancos": "votos_blancos",
        "Candidato": "candidato",
        "Mesa": "mesa"
    })
    
    # Asegurar tipos correctos
    df = df.with_columns([
        pl.col("votos").cast(pl.Int64),
        pl.col("votos_nulos").cast(pl.Int64),
        pl.col("votos_blancos").cast(pl.Int64)
    ])
    
    return df

if __name__ == "__main__":
    import load_raw
    df = load_raw.load_files("data/")
    df_clean = clean_data(df)
    print(df_clean.head())
