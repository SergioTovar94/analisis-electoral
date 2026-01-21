import polars as pl
import chardet
from pathlib import Path
import json
import sys
from collections import defaultdict
import csv

# CATEGORÍAS COMPLETAS - TÚ DECIDES QUÉ ES CADA UNA
CATEGORIAS = {
    # Ubicación geográfica
    '1': 'departamento',
    '2': 'municipio',
    '3': 'puesto_votacion',
    '4': 'mesa',
    '5': 'zona',
    '6': 'comuna',
    '7': 'circunscripcion',
    '8': 'localidad',
    
    # Códigos de ubicación
    '9': 'cod_departamento',
    '10': 'cod_municipio',
    '11': 'cod_puesto',
    '12': 'cod_mesa',
    '13': 'cod_zona',
    '14': 'cod_comuna',
    '15': 'cod_circunscripcion',
    '16': 'cod_localidad',
    
    # Candidatos y votos
    '17': 'candidato',
    '18': 'cod_candidato',
    '19': 'partido',
    '20': 'cod_partido',
    '21': 'votos',
    
    # Metadatos electorales
    '22': 'anio_eleccion',
    
    # Códigos misceláneos
    '23': 'cod_proceso',
    '24': 'cod_eleccion',
    '25': 'cod_corporacion',
    '26': 'cod_categoria',
    
    # Final
    '99': 'otro',
    '0': 'ignorar'  # Para columnas que no quieres cargar
}

def mostrar_menu_categorias():
    """Muestra las categorías organizadas por grupos"""
    print("\n" + "═" * 80)
    print("📋 CATEGORÍAS DISPONIBLES - USAR NÚMEROS")
    print("═" * 80)
    
    grupos = {
        "📍 UBICACIÓN": ['1', '2', '3', '4', '5', '6', '7', '8'],
        "🔢 CÓDIGOS UBICACIÓN": ['9', '10', '11', '12', '13', '14', '15', '16'],
        "👥 CANDIDATOS/VOTOS": ['17', '18', '19', '20', '21'],
        "🗳️ ANIO": ['22'],
        "📅 MISCELANEOS": ['23', '24', '25', '26'],
        "⚙️ OTROS": ['99', '0']
    }
    
    for grupo_nombre, numeros in grupos.items():
        print(f"\n{grupo_nombre}:")
        for num in numeros:
            print(f"  [{num:>2}] {CATEGORIAS[num]}")
    
    print("═" * 80)

def detectar_encoding(file_path):
    """Detectar encoding del archivo"""
    with open(file_path, 'rb') as f:
        rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        return result['encoding'] or 'utf-8'
    
def detectar_delimitador(file_path):
    """Detecta automáticamente el delimitador del CSV"""
    with open(file_path, 'r', encoding=detectar_encoding(file_path), errors='ignore') as f:
        sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample)
            return dialect.delimiter
        except Exception:
            # Si falla, asumimos coma
            return ','

def leer_csv_seguro(file_path, n_rows=None, columnas=None):
    """Leer CSV de forma segura forzando todo a string, detectando delimitador"""
    encoding = detectar_encoding(file_path)
    delimiter = detectar_delimitador(file_path)
    
    # PRIMERO: Leer solo los nombres de columnas
    try:
        df_columns = pl.read_csv(file_path, n_rows=0, encoding=encoding, separator=delimiter)
        todas_las_columnas = df_columns.columns
        
        # Si se especifican columnas, filtrar solo las que existen
        if columnas:
            columnas_existentes = [col for col in columnas if col in todas_las_columnas]
            if not columnas_existentes:
                print(f"⚠️ Ninguna de las columnas solicitadas existe en el archivo")
                return None
        else:
            columnas_existentes = todas_las_columnas
        
        # Forzar TODAS las columnas a string
        schema = {col: pl.Utf8 for col in columnas_existentes}
        
        # Leer el CSV completo
        df = pl.read_csv(
            file_path,
            encoding=encoding,
            schema_overrides=schema,
            ignore_errors=True,
            n_rows=n_rows,
            columns=columnas_existentes if columnas_existentes != todas_las_columnas else None,
            separator=delimiter,
            truncate_ragged_lines=True
        )
        return df
        
    except Exception as e:
        print(f"❌ Error leyendo CSV: {e}")
        # Intento de emergencia
        try:
            print("🔧 Intentando lectura de emergencia...")
            df = pl.read_csv(
                file_path,
                encoding=encoding,
                has_header=True,
                infer_schema_length=0,
                ignore_errors=True,
                n_rows=n_rows,
                separator=delimiter,
                truncate_ragged_lines=True
            )
            # Convertir todas las columnas a string
            for col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))
            return df
        except Exception as e2:
            print(f"💥 Error en lectura de emergencia: {e2}")
            return None

def mapeo_manual_columnas(file_path):
    """Mapeo manual completo - versión mejorada"""
    # Leer archivo para preview
    if file_path.suffix.lower() == ".csv":
        df = leer_csv_seguro(file_path, n_rows=5)
        if df is None:
            print(f"   ❌ No se pudo leer el archivo para preview")
            return None
        cols = df.columns
    else:
        try:
            df = pl.read_excel(file_path, n_rows=5)
            cols = df.columns
        except Exception as e:
            print(f"   ❌ Error leyendo Excel: {e}")
            return None
    
    print(f"\n{'='*80}")
    print(f"📄 ARCHIVO: {file_path.name}")
    print(f"📏 {len(cols)} columnas, {df.shape[0]} filas de preview")
    print("="*80)
    
    # Mostrar vista previa
    print("\n📊 VISTA PREVIA DE DATOS (primeras 3 filas):")
    print(df.head(3))
    
    print("\n" + "="*80)
    print("🎯 ASIGNACIÓN MANUAL DE COLUMNAS")
    print("="*80)
    
    mapping = {}
    
    for i, col in enumerate(cols, 1):
        print(f"\n{'─'*40}")
        print(f"COLUMNA {i}/{len(cols)}: '{col}'")
        print(f"{'─'*40}")
        
        # Mostrar valores de ejemplo (truncados si son muy largos)
        valores = []
        for val in df[col].head(3).to_list():
            str_val = str(val)
            if len(str_val) > 30:
                valores.append(str_val[:27] + "...")
            else:
                valores.append(str_val)
        print(f"📝 Ejemplos: {valores}")
        
        mostrar_menu_categorias()
        
        while True:
            try:
                respuesta = input(f"\nNúmero de categoría para '{col}' (0-29, 99=otro, 0=ignorar): ").strip()
                
                if respuesta == '':
                    print("⚠️  Debes ingresar un número. Intenta de nuevo.")
                    continue
                
                if respuesta in CATEGORIAS:
                    categoria = CATEGORIAS[respuesta]
                    mapping[col] = categoria
                    
                    if respuesta == '0':
                        print(f"   ⚫ '{col}' será IGNORADA")
                    else:
                        print(f"   ✅ '{col}' → {categoria}")
                    break
                else:
                    print(f"❌ Número inválido. Opciones: 0-29, 99")
                    
            except (EOFError, KeyboardInterrupt):
                print("\n\n⚠️  Proceso interrumpido por usuario")
                return None
    
    # Resumen del mapeo
    print(f"\n{'='*80}")
    print("📋 RESUMEN DE MAPEO")
    print("="*80)
    
    por_categoria = defaultdict(list)
    for col, cat in mapping.items():
        por_categoria[cat].append(col)
    
    for cat in sorted(por_categoria.keys()):
        print(f"\n{cat.upper()}:")
        for col in por_categoria[cat]:
            print(f"  • {col}")
    
    # Confirmación
    print(f"\n{'='*80}")
    confirmacion = input("¿Confirmar este mapeo? (s/n): ").strip().lower()
    
    if confirmacion in ['s', 'si', 'sí', 'yes', 'y']:
        return mapping
    else:
        print("🔄 Reiniciando mapeo para este archivo...")
        return mapeo_manual_columnas(file_path)

def cargar_archivo_completo(file_path, mapping):
    """Cargar archivo completo - versión robusta"""
    print(f"   📂 Cargando: {file_path.name}")
    
    # Filtrar columnas a cargar
    columnas_a_cargar = [col for col, cat in mapping.items() if cat != 'ignorar']
    
    if not columnas_a_cargar:
        print(f"   ⚠️  Todas las columnas marcadas para ignorar")
        return None
    
    try:
        if file_path.suffix.lower() == ".csv":
            df = leer_csv_seguro(file_path, columnas=columnas_a_cargar)
        else:
            df = pl.read_excel(file_path)
            columnas_disponibles = [col for col in columnas_a_cargar if col in df.columns]
            if not columnas_disponibles:
                print(f"   ⚠️  Ninguna columna disponible después de filtrar")
                return None
            df = df.select(columnas_disponibles)
        
        if df is None or df.is_empty():
            print(f"   ⚠️  No se pudieron cargar datos")
            return None
        
        print(f"   ✅ {df.shape[0]:,} filas, {df.shape[1]} columnas cargadas")
        
        # Renombrar columnas importantes
        rename_dict = {}
        for col_original in df.columns:
            if col_original in mapping and mapping[col_original] not in ['ignorar', 'otro']:
                rename_dict[col_original] = mapping[col_original]
        
        if rename_dict:
            df = df.rename(rename_dict)
            print(f"   🔄 {len(rename_dict)} columnas renombradas")

        columnas_finales = [
            mapping[col]
            for col in mapping
            if mapping[col] not in ('ignorar', 'otro')
            and mapping[col] in df.columns
        ]
        ORDEN_COLUMNAS = [
            "anio_eleccion",
            "cod_departamento", "departamento",
            "cod_municipio", "municipio",
            "cod_zona",
            "cod_comuna", "comuna",
            "cod_puesto", "puesto_votacion",
            "mesa",
            "cod_partido", "partido",
            "cod_candidato", "candidato",
            "votos"
        ]
        columnas_finales = [c for c in ORDEN_COLUMNAS if c in df.columns]
        df = df.select(columnas_finales)
        
        return df
        
    except Exception as e:
        print(f"   ❌ Error al cargar: {e}")
        return None

def proceso_completo_manual(data_dir: str):
    """Proceso completo mejorado"""
    data_path = Path(data_dir)
    archivos = list(data_path.glob("*.*"))
    
    if not archivos:
        raise ValueError(f"❌ No hay archivos en {data_dir}")
    
    print(f"📁 Encontrados {len(archivos)} archivos:")
    for archivo in archivos:
        print(f"  • {archivo.name}")
    
    resultados = []
    mapeos_totales = {}
    
    for archivo in archivos:
        print(f"\n{'='*80}")
        print(f"📄 PROCESANDO: {archivo.name}")
        print(f"{'='*80}")
        
        # Mapeo manual
        mapping = mapeo_manual_columnas(archivo)
        if mapping is None:
            print(f"⏭️  Saltando {archivo.name}")
            continue
        
        mapeos_totales[archivo.name] = mapping
        
        # Carga completa
        df = cargar_archivo_completo(archivo, mapping)
        if df is None:
            print(f"⚠️  No se pudo cargar {archivo.name}")
            continue
        # Verificar anio_eleccion
        if "anio_eleccion" not in df.columns:
            print("\n⚠️  El dataset NO contiene la columna 'anio_eleccion'")
            print(f"📄 Archivo: {archivo.name}")

            while True:
                anio_input = input("👉 Ingresa el año de la elección (YYYY): ").strip()
                if not anio_input.isdigit() or len(anio_input) != 4:
                    print("❌ Año inválido. Debe ser un número de 4 dígitos (ej: 2023)")
                    continue
                anio = int(anio_input)
                break

            df = df.with_columns(pl.lit(str(anio)).alias("anio_eleccion"))
            print(f"✅ Año {anio} asignado manualmente")

        # Agregar metadatos
        df = df.with_columns([
            pl.lit(archivo.name).alias("_archivo_fuente"),
            pl.lit(archivo.stem).alias("_dataset")
        ])
        
        resultados.append(df)
        print(f"✅ {archivo.name} procesado")
    
    if not resultados:
        raise ValueError("❌ No se procesó ningún archivo")
    
    # Consolidar
    print(f"\n{'='*80}")
    print("📊 CONSOLIDANDO DATASETS")
    print("="*80)
    
    if len(resultados) == 1:
        final_df = resultados[0]
    else:
        final_df = pl.concat(resultados, how="diagonal")
    
    print(f"🎉 DATASET FINAL CREADO")
    print(f"   📈 {final_df.shape[0]:,} filas totales")
    print(f"   📊 {final_df.shape[1]} columnas totales")
    print(f"   📂 {len(resultados)} archivos consolidados")
    
    return final_df, mapeos_totales

def guardar_resultados(df, mapeos):
    """Guardar resultados"""
    output_dir = Path("data_processed")
    output_dir.mkdir(exist_ok=True)
    
    # Dataset
    csv_path = output_dir / "dataset_consolidado.csv"
    df.write_csv(csv_path)
    print(f"\n💾 Dataset: {csv_path}")
    
    # Mapeos
    json_path = output_dir / "mapeos.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(mapeos, f, indent=2, ensure_ascii=False)
    print(f"💾 Mapeos: {json_path}")
    
    # Vista previa
    preview_path = output_dir / "preview.txt"
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(f"Dataset consolidado\n")
        f.write(f"Filas: {df.shape[0]:,}\n")
        f.write(f"Columnas: {df.shape[1]}\n\n")
        f.write("Columnas disponibles:\n")
        for col in df.columns:
            f.write(f"  • {col}\n")
        f.write("\nPrimeras 10 filas:\n")
        f.write(str(df.head(10)))
    
    print(f"💾 Preview: {preview_path}")
    
    return output_dir

if __name__ == "__main__":
    try:
        print("🎯 SISTEMA DE CARGA MANUAL MEJORADO")
        print("="*80)
        
        df_final, mapeos = proceso_completo_manual("data_raw/")
        
        print(f"\n{'='*80}")
        print("✅ PROCESO COMPLETADO")
        print("="*80)
        
        # Vista previa
        print("\n👀 VISTA PREVIA FINAL:")
        print(df_final.head(10))
        
        # Guardar
        output_dir = guardar_resultados(df_final, mapeos)
        
        print(f"\n{'='*80}")
        print(f"📁 Resultados en: {output_dir.absolute()}/")
        print("="*80)
        
        # Estadísticas
        print("\n📊 ESTADÍSTICAS:")
        columnas_importantes = [c for c in df_final.columns if not c.startswith('_')]
        print(f"Columnas de datos: {len(columnas_importantes)}")
        
        for cat in ['departamento', 'municipio', 'candidato', 'votos']:
            if cat in df_final.columns:
                print(f"  • {cat}: {df_final[cat].n_unique():,} valores únicos")
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ ERROR: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        sys.exit(1)