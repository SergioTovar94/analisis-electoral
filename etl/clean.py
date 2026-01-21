import polars as pl

def clean_schema(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        c: c.lower().strip().replace(" ", "_")
        for c in df.columns
    })

    numeric_cols = ["votos"]

    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .cast(pl.Int64, strict=False)
                .fill_null(0)
            )

    return df
