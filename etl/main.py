import polars as pl
from clean import clean_schema
from filter import interactive_filter
import sys
from pathlib import Path


def load_consolidated_data(file_path: str = "data_processed/dataset_consolidado.csv"):
    """Cargar dataset consolidado de forma segura"""
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"❌ Error: No se encontró el archivo {file_path}")
        print("   Ejecuta primero: python etl/load_raw.py")
        sys.exit(1)
    
    print(f"📂 Cargando dataset consolidado...")
    
    # Primero leer solo los nombres de columnas
    try:
        column_names = pl.read_csv(file_path, n_rows=0).columns
        print(f"   ✓ {len(column_names)} columnas detectadas")
    except Exception as e:
        print(f"❌ Error al leer columnas: {e}")
        sys.exit(1)
    
    # Forzar todas las columnas a string para evitar problemas
    schema_overrides = {col: pl.Utf8 for col in column_names}
    
    try:
        df = pl.read_csv(
            file_path,
            schema_overrides=schema_overrides,
            ignore_errors=True
        )
        print(f"   ✅ {df.shape[0]:,} filas cargadas")
        return df
    except Exception as e:
        print(f"❌ Error al cargar datos: {e}")
        # Fallback: intentar leer sin schema_overrides
        df = pl.read_csv(file_path, ignore_errors=True)
        print(f"   ⚠️  Cargado en modo de emergencia: {df.shape[0]:,} filas")
        return df

if __name__ == "__main__":
    df = load_consolidated_data()

    print("🧹 Limpieza estructural")
    df = clean_schema(df)

    if input("¿Aplicar filtro? (s/n): ").lower() in ("s", "si", "sí"):
        df = interactive_filter(df)

    print("\n👀 Preview final:")
    print(df.head(10))
    output_dir = Path("data_processed")
    output_dir.mkdir(exist_ok=True)
    csv_path = output_dir / "dataset_filtrado.csv"
    df.write_csv(csv_path)
    print(f"\n💾 Dataset: {csv_path}")
