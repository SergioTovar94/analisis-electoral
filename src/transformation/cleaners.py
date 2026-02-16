import pandas as pd

def clean_ventas(df):
    # Eliminar registros con valores nulos críticos
    df = df.dropna(subset=["id_venta", "monto"])
    # Convertir fechas
    df["fecha"] = pd.to_datetime(df["fecha"])
    return df