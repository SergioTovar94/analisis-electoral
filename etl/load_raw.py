import polars as pl
from pathlib import Path

def preview_file(file_path):
    # Cargar archivo sin cambios
    if file_path.suffix == ".csv":
        df = pl.read_csv(file_path, n_rows=5)
    else:
        df = pl.read_excel(file_path, n_rows=5)
    
    print(f"\nArchivo: {file_path.name}")
    print("Columnas detectadas:", df.columns)
    print("Primeras filas:")
    print(df.head())
    
    # Mapping interactivo
    mapping = {}
    for col in df.columns:
        valor = input(f"¿Qué representa la columna '{col}'? (departamento/municipio/puesto/candidato/votos/votos_nulos/votos_blancos/mesa/otro): ")
        mapping[col] = valor
    return mapping

def load_and_standardize(data_dir: str):
    all_files = Path(data_dir).glob("*.*")
    dfs = []
    mappings = {}
    
    for f in all_files:
        mapping = preview_file(f)
        mappings[f.name] = mapping
        
        # Leer todo el archivo completo
        if f.suffix == ".csv":
            df = pl.read_csv(f)
        else:
            df = pl.read_excel(f)
        
        # Renombrar columnas según mapping
        rename_dict = {col: mapping[col] for col in df.columns if mapping[col] in [
            "departamento","municipio","puesto","candidato","votos","votos_nulos","votos_blancos","mesa"
        ]}
        df = df.rename(rename_dict)
        
        dfs.append(df)
    
    return pl.concat(dfs), mappings

if __name__ == "__main__":
    df_raw, mappings = load_and_standardize("data/")
    print("\nMapping generado por archivo:")
    for f, m in mappings.items():
        print(f"{f}: {m}")
    print("\nPrimeras filas del dataset final concatenado:")
    print(df_raw.head())
