import polars as pl

def interactive_filter(df: pl.DataFrame) -> pl.DataFrame:
    print("\n📌 COLUMNAS DISPONIBLES:")
    for i, col in enumerate(df.columns, 1):
        print(f"[{i}] {col}")

    idx = int(input("\nNúmero de columna: ")) - 1
    column = df.columns[idx]

    uniques = df[column].unique().sort().head(100)
    print(f"\nValores de ejemplo en '{column}':")
    for v in uniques:
        print(f"  • {v}")

    value = input(f"\nValor exacto para filtrar '{column}': ").strip()

    return df.filter(pl.col(column) == value)
