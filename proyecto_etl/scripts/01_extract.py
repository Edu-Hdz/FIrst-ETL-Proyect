"""
Módulo de Extracción: Lee el archivo Excel y realiza limpieza inicial
"""

import pandas as pd
import numpy as np
import os
from config import EXCEL_FILE, CSV_FILE

def extract_excel(EXCEL_FILE):
    """
    Extrae datos del archivo Excel
    """
    try:
        # Verificar si el archivo existe
        if not os.path.exists(EXCEL_FILE):
            print(f" El archivo no existe en: {EXCEL_FILE}")
            print(f" Directorio actual: {os.getcwd()}")
            return None
        
        # Leer el archivo Excel (especificando engine para archivos .xls)
        df = pd.read_excel(EXCEL_FILE, engine='xlrd')  # Para archivos .xls
        print(f" Datos extraídos correctamente: {len(df)} filas")
        print(f" Columnas encontradas: {list(df.columns)}")
        return df
    
    except FileNotFoundError:
        print(f" Archivo no encontrado: {EXCEL_FILE}")
        return None
    except Exception as e:
        print(f" Error al leer el archivo: {e}")
        return None

def inspect_data(df):
    """
    Inspecciona la calidad de los datos
    """
    print("\n" + "="*50)
    print("INSPECCIÓN DE DATOS")
    print("="*50)
    
    print("\n Primeras 5 filas:")
    print(df.head())
    
    print("\n Información del DataFrame:")
    print(df.info())
    
    print("\n Valores Nulos:")
    print(df.isnull().sum())
    
    print("\n Datos duplicados:")
    print(f"Filas duplicadas: {df.duplicated().sum()}")
    
    print("\n Estadísticas básicas:")
    print(df.describe())
    
    return df.isnull().sum()  # Retornar valores nulos para análisis

def save_to_csv(df, CSV_FILE):
    """
    Guarda el DataFrame a CSV
    """
    try:
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
        
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"\n Datos guardados en: {CSV_FILE}")
    except Exception as e:
        print(f" Error al guardar CSV: {e}")

if __name__ == "__main__":
    # Ejecutar extracción
    df = extract_excel(EXCEL_FILE)
    
    if df is not None:
        inspect_data(df)
        save_to_csv(df, CSV_FILE)


