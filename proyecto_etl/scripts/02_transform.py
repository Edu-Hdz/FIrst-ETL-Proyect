"""
Módulo de Transformación: Normaliza los datos (1FN, 2FN, 3FN)
"""

import pandas as pd
import numpy as np
import re
from config import CSV_FILE, EXCEL_FILE

class DataNormalizer:
    """
    Clase para normalizar datos según las formas normales
    """
    
    def __init__(self, df):
        self.df_original = df
        self.df_normalized = None
    
    def step1_clean_data(self):
        """
        Limpieza inicial de datos
        """
        df = self.df_original.copy()
        
        # Convertir a mayúsculas iniciales los nombres (CORREGIDO: .title() no .tittle())
        if 'Vendedor' in df.columns:
            df['Vendedor'] = df['Vendedor'].astype(str).str.title()
        
        # Limpiar espacios en blanco (CORREGIDO: variable col definida)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
        
        # Limpiar espacios en columnas de texto
        if 'Tipo' in df.columns:
            df['Tipo'] = df['Tipo'].astype(str).str.strip()
        if 'Operación' in df.columns:
            df['Operación'] = df['Operación'].astype(str).str.strip()
        if 'Provincia' in df.columns:
            df['Provincia'] = df['Provincia'].astype(str).str.strip()
        
        # Convertir fechas a formato datetime
        if 'Fecha Alta' in df.columns:
            df['Fecha Alta'] = pd.to_datetime(df['Fecha Alta'], errors='coerce')
        
        if 'Fecha Venta' in df.columns:
            df['Fecha Venta'] = pd.to_datetime(df['Fecha Venta'], errors='coerce')
        
        # Convertir Superficie a numérico
        if 'Superficie' in df.columns:
            df['Superficie'] = pd.to_numeric(df['Superficie'], errors='coerce')
        
        # Convertir Precio Venta a numérico
        if 'Precio Venta' in df.columns:
            df['Precio Venta'] = pd.to_numeric(df['Precio Venta'], errors='coerce')
        
        print(" Paso 1: Limpieza inicial completada")
        print(f"   - Registros después de limpieza: {len(df)}")
        print(f"   - Valores nulos en Fecha Venta: {df['Fecha Venta'].isnull().sum()}")
        print(f"   - Valores nulos en Vendedor: {df['Vendedor'].isnull().sum()}")
        
        return df
    
    def step2_handle_null_values(self, df):
        """
        Paso 2: Manejar valores nulos (sin expandir columnas porque ya son atómicas)
        """
        df_clean = df.copy()
        
        # Opción 1: Eliminar filas donde Fecha Venta y Vendedor son nulos (propiedades no vendidas)
        # Para propiedades vendidas, mantenemos los datos aunque tengan valores nulos en otras columnas
        
        print("\n Análisis de propiedades vendidas vs no vendidas:")
        vendidas = df_clean[df_clean['Fecha Venta'].notna()]
        no_vendidas = df_clean[df_clean['Fecha Venta'].isna()]
        
        print(f"   - Propiedades VENDIDAS: {len(vendidas)} registros")
        print(f"   - Propiedades NO VENDIDAS: {len(no_vendidas)} registros")
        
        # No eliminamos las propiedades no vendidas, las mantenemos con Fecha Venta = NULL
        # Para propiedades no vendidas, Vendedor puede ser NULL
        
        # Convertir cantidades a números (asegurar)
        if 'Superficie' in df_clean.columns:
            df_clean['Superficie'] = pd.to_numeric(df_clean['Superficie'], errors='coerce')
        
        if 'Precio Venta' in df_clean.columns:
            df_clean['Precio Venta'] = pd.to_numeric(df_clean['Precio Venta'], errors='coerce')
        
        print(f"\n Paso 2: Manejo de valores nulos completado")
        print(f"   - Total registros: {len(df_clean)}")
        print(f"   - Registros con vendedor: {df_clean['Vendedor'].notna().sum()}")
        
        return df_clean
    
    def step3_create_entities(self, df):
        """
        Paso 3: Crear entidades separadas (Segunda y Tercera Forma Normal)
        """
        
        print("\n" + "="*50)
        print("CREANDO ENTIDADES NORMALIZADAS")
        print("="*50)
        
        # ============================================
        # 3.1 ENTIDAD: VENDEDORES
        # ============================================
        # Extraer vendedores únicos (solo los que tienen valor)
        vendedores_df = df[df['Vendedor'].notna()][['Vendedor']].drop_duplicates().reset_index(drop=True)
        vendedores_df['id_vendedor'] = vendedores_df.index + 1
        
        # Crear mapeo de vendedor a ID
        vendedor_map = dict(zip(vendedores_df['Vendedor'], vendedores_df['id_vendedor']))
        
        # Agregar id_vendedor al DataFrame principal
        df['id_vendedor'] = df['Vendedor'].map(vendedor_map)
        
        print(f" Vendedores únicos: {len(vendedores_df)}")
        
        # ============================================
        # 3.2 ENTIDAD: PROPIEDADES (bienes raíces)
        # ============================================
        # La propiedad se identifica por Referencia (única)
        propiedades_df = df[['Referencia', 'Fecha Alta', 'Tipo', 'Operación', 'Provincia', 'Superficie', 'Precio Venta']].copy()
        propiedades_df = propiedades_df.drop_duplicates(subset=['Referencia']).reset_index(drop=True)
        
        print(f" Propiedades únicas: {len(propiedades_df)}")
        
        # ============================================
        # 3.3 ENTIDAD: VENTAS (transacciones)
        # ============================================
        # Solo propiedades que tienen fecha de venta
        ventas_df = df[df['Fecha Venta'].notna()][['Referencia', 'Fecha Venta', 'id_vendedor']].copy()
        ventas_df = ventas_df.drop_duplicates().reset_index(drop=True)
        ventas_df['id_venta'] = ventas_df.index + 1
        
        # Crear mapeo de Referencia a venta (si hay múltiples ventas por propiedad)
        # Para simplificar, asumimos que cada propiedad se vende una vez
        venta_map = dict(zip(ventas_df['Referencia'], ventas_df['id_venta']))
        df['id_venta'] = df['Referencia'].map(venta_map)
        
        print(f" Ventas registradas: {len(ventas_df)}")
        
        # ============================================
        # 3.4 ENTIDAD: DETALLE_VENTA (opcional, si hay múltiples ítems por venta)
        # En este caso, cada venta es por una propiedad, así que es relación 1:1
        # ============================================
        detalle_venta_df = ventas_df[['id_venta', 'Referencia']].copy()
        
        # Limpiar nombres de columnas para MySQL
        propiedades_df = propiedades_df.rename(columns={
            'Referencia': 'referencia',
            'Fecha Alta': 'fecha_alta',
            'Tipo': 'tipo',
            'Operación': 'operacion',
            'Provincia': 'provincia',
            'Superficie': 'superficie',
            'Precio Venta': 'precio_venta'
        })
        
        vendedores_df = vendedores_df.rename(columns={
            'Vendedor': 'nombre'
        })
        
        ventas_df = ventas_df.rename(columns={
            'Referencia': 'referencia',
            'Fecha Venta': 'fecha_venta',
            'id_vendedor': 'id_vendedor',
            'id_venta': 'id_venta'
        })
        
        detalle_venta_df = detalle_venta_df.rename(columns={
            'id_venta': 'id_venta',
            'Referencia': 'referencia'
        })
        
        print(f"\n Entidades creadas:")
        print(f"   - Vendedores: {len(vendedores_df)}")
        print(f"   - Propiedades: {len(propiedades_df)}")
        print(f"   - Ventas: {len(ventas_df)}")
        print(f"   - Detalle Ventas: {len(detalle_venta_df)}")
        
        return {
            'vendedores': vendedores_df,
            'propiedades': propiedades_df,
            'ventas': ventas_df,
            'detalle_venta': detalle_venta_df
        }
    
    def run(self):
        """
        Ejecuta todo el proceso de normalización
        """
        print("\n" + "="*60)
        print("INICIANDO PROCESO DE NORMALIZACIÓN")
        print("="*60)
        
        # Paso 1: Limpieza
        df_clean = self.step1_clean_data()
        
        # Paso 2: Manejo de valores nulos
        df_clean = self.step2_handle_null_values(df_clean)
        
        # Paso 3: Crear entidades (2FN y 3FN)
        entities = self.step3_create_entities(df_clean)
        
        return entities

def transform_main():
    """
    Función principal de transformación
    """
    # Leer datos del CSV generado en extracción
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8')
        print(f" Datos cargados desde: {CSV_FILE}")
        print(f" Registros: {len(df)}")
    except FileNotFoundError:
        print(f" No se encontró {CSV_FILE}, intentando leer desde Excel...")
        try:
            df = pd.read_excel(EXCEL_FILE, engine='xlrd')
            print(f" Datos cargados desde: {EXCEL_FILE}")
        except Exception as e:
            print(f" Error al cargar datos: {e}")
            return None
    
    # Normalizar
    normalizer = DataNormalizer(df)
    entities = normalizer.run()
    
    return entities

if __name__ == "__main__":
    entities = transform_main()
    
    # Mostrar resultados
    if entities:
        print("\n" + "="*60)
        print("RESULTADO DE LA NORMALIZACIÓN")
        print("="*60)
        
        for entity_name, entity_df in entities.items():
            print(f"\n TABLA: {entity_name.upper()}")
            print(entity_df.head())
            print(f"   Total: {len(entity_df)} registros")



        