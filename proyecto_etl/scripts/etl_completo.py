"""
ETL COMPLETO: Excel → Python → MySQL
Ejecuta todo el proceso de extracción, transformación y carga
"""

import sys
import os
import warnings
warnings.filterwarnings('ignore')

# ============================================
# IMPORTANTE: Agregar el directorio padre al path
# ============================================
# Obtener la ruta del directorio donde está este script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta del directorio padre (proyecto_etl)
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
# Agregar el directorio padre al path de Python
sys.path.insert(0, PARENT_DIR)
# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import EXCEL_FILE, CSV_FILE, DB_CONFIG
import pandas as pd
from sqlalchemy import create_engine, text

# ============================================
# 1. EXTRACCIÓN
# ============================================
def extract():
    """Extrae datos del archivo Excel"""
    print("\n" + "="*60)
    print("FASE 1: EXTRACCIÓN")
    print("="*60)
    
    try:
        # Verificar si el archivo existe
        if not os.path.exists(EXCEL_FILE):
            print(f" Archivo no encontrado: {EXCEL_FILE}")
            print(f" Directorio actual: {os.getcwd()}")
            return None
        
        df = pd.read_excel(EXCEL_FILE, engine='xlrd')
        print(f" Datos extraídos: {len(df)} filas, {len(df.columns)} columnas")
        print(f" Columnas: {list(df.columns)}")
        
        # Mostrar valores nulos
        print("\n Valores nulos por columna:")
        for col in df.columns:
            nulos = df[col].isnull().sum()
            print(f"   - {col}: {nulos} ({nulos/len(df)*100:.1f}%)")
        
        return df
    except Exception as e:
        print(f" Error en extracción: {e}")
        return None

# ============================================
# 2. TRANSFORMACIÓN
# ============================================
def transform(df):
    """Transforma y normaliza los datos"""
    print("\n" + "="*60)
    print("FASE 2: TRANSFORMACIÓN (Normalización)")
    print("="*60)
    
    # ========== LIMPIEZA INICIAL ==========
    df = df.copy()
    
    # Convertir texto a título
    if 'Vendedor' in df.columns:
        df['Vendedor'] = df['Vendedor'].astype(str).str.title()
    
    # Limpiar espacios
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    # Convertir fechas
    if 'Fecha Alta' in df.columns:
        df['Fecha Alta'] = pd.to_datetime(df['Fecha Alta'], errors='coerce')
    if 'Fecha Venta' in df.columns:
        df['Fecha Venta'] = pd.to_datetime(df['Fecha Venta'], errors='coerce')
    
    # Convertir números
    if 'Superficie' in df.columns:
        df['Superficie'] = pd.to_numeric(df['Superficie'], errors='coerce')
    if 'Precio Venta' in df.columns:
        df['Precio Venta'] = pd.to_numeric(df['Precio Venta'], errors='coerce')
    
    print(" Limpieza inicial completada")
    
    # ========== ANÁLISIS DE VENTAS ==========
    vendidas = df[df['Fecha Venta'].notna()]
    no_vendidas = df[df['Fecha Venta'].isna()]
    
    print(f"\n Análisis de propiedades:")
    print(f"   - Propiedades VENDIDAS: {len(vendidas)}")
    print(f"   - Propiedades NO VENDIDAS: {len(no_vendidas)}")
    
    # ========== CREAR ENTIDADES ==========
    print("\n" + "="*50)
    print("CREANDO ENTIDADES NORMALIZADAS")
    print("="*50)
    
    # 1. VENDEDORES
    vendedores_df = df[df['Vendedor'].notna()][['Vendedor']].drop_duplicates().reset_index(drop=True)
    vendedores_df['id_vendedor'] = vendedores_df.index + 1
    vendedor_map = dict(zip(vendedores_df['Vendedor'], vendedores_df['id_vendedor']))
    df['id_vendedor'] = df['Vendedor'].map(vendedor_map)
    vendedores_df = vendedores_df.rename(columns={'Vendedor': 'nombre'})
    print(f" Vendedores: {len(vendedores_df)}")
    
    # 2. PROPIEDADES
    propiedades_df = df[['Referencia', 'Fecha Alta', 'Tipo', 'Operación', 'Provincia', 'Superficie', 'Precio Venta']].copy()
    propiedades_df = propiedades_df.drop_duplicates(subset=['Referencia']).reset_index(drop=True)
    propiedades_df = propiedades_df.rename(columns={
        'Referencia': 'referencia',
        'Fecha Alta': 'fecha_alta',
        'Tipo': 'tipo',
        'Operación': 'operacion',
        'Provincia': 'provincia',
        'Superficie': 'superficie',
        'Precio Venta': 'precio_venta'
    })
    print(f" Propiedades: {len(propiedades_df)}")
    
    # 3. VENTAS
    ventas_df = df[df['Fecha Venta'].notna()][['Referencia', 'Fecha Venta', 'id_vendedor']].copy()
    ventas_df = ventas_df.drop_duplicates().reset_index(drop=True)
    ventas_df['id_venta'] = ventas_df.index + 1
    ventas_df = ventas_df.rename(columns={
        'Referencia': 'referencia',
        'Fecha Venta': 'fecha_venta'
    })
    print(f" Ventas: {len(ventas_df)}")
    
    # 4. DETALLE_VENTA
    detalle_venta_df = ventas_df[['id_venta', 'referencia']].copy()
    print(f" Detalle Ventas: {len(detalle_venta_df)}")
    
    entities = {
        'vendedores': vendedores_df,
        'propiedades': propiedades_df,
        'ventas': ventas_df,
        'detalle_venta': detalle_venta_df
    }
    
    return entities

# ============================================
# 3. CARGA EN MYSQL
# ============================================
def load(entities):
    """Carga los datos en MySQL"""
    print("\n" + "="*60)
    print("FASE 3: CARGA EN MYSQL")
    print("="*60)
    
    # Conectar a MySQL
    db_url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    engine = create_engine(db_url)
    
    # Crear base de datos
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}"))
        conn.commit()
    print(f" Base de datos '{DB_CONFIG['database']}' creada/verificada")
    
    # Reconectar a la base específica
    db_url_full = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url_full)
    
    # Crear tablas
    create_sql = """
    CREATE TABLE IF NOT EXISTS vendedores (
        id_vendedor INT AUTO_INCREMENT PRIMARY KEY,
        nombre VARCHAR(100) NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS propiedades (
        referencia VARCHAR(50) PRIMARY KEY,
        fecha_alta DATE NOT NULL,
        tipo VARCHAR(50),
        operacion VARCHAR(50),
        provincia VARCHAR(100),
        superficie DECIMAL(10,2),
        precio_venta DECIMAL(12,2)
    );
    
    CREATE TABLE IF NOT EXISTS ventas (
        id_venta INT AUTO_INCREMENT PRIMARY KEY,
        referencia VARCHAR(50) NOT NULL,
        fecha_venta DATE NOT NULL,
        id_vendedor INT,
        FOREIGN KEY (referencia) REFERENCES propiedades(referencia) ON DELETE CASCADE,
        FOREIGN KEY (id_vendedor) REFERENCES vendedores(id_vendedor) ON DELETE SET NULL
    );
    
    CREATE TABLE IF NOT EXISTS detalle_venta (
        id_venta INT PRIMARY KEY,
        referencia VARCHAR(50) NOT NULL,
        FOREIGN KEY (id_venta) REFERENCES ventas(id_venta) ON DELETE CASCADE,
        FOREIGN KEY (referencia) REFERENCES propiedades(referencia) ON DELETE CASCADE
    );
    """
    
    for statement in create_sql.split(';'):
        if statement.strip():
            with engine.begin() as conn:
                conn.execute(text(statement))
    print(" Tablas creadas correctamente")
    
    # Cargar datos
    load_order = ['vendedores', 'propiedades', 'ventas', 'detalle_venta']
    for table_name in load_order:
        if table_name in entities:
            df = entities[table_name]
            try:
                df.to_sql(table_name, engine, if_exists='append', index=False)
                print(f" Datos cargados en '{table_name}': {len(df)} registros")
            except Exception as e:
                print(f" Error en '{table_name}': {e}")
    
    # Verificar
    print("\n" + "="*60)
    print("VERIFICACIÓN FINAL")
    print("="*60)
    
    query = """
    SELECT 
        v.id_venta,
        v.fecha_venta,
        p.referencia,
        p.tipo,
        p.provincia,
        p.precio_venta,
        ve.nombre AS vendedor
    FROM ventas v
    JOIN propiedades p ON v.referencia = p.referencia
    LEFT JOIN vendedores ve ON v.id_vendedor = ve.id_vendedor
    ORDER BY v.fecha_venta DESC
    LIMIT 10;
    """
    
    try:
        result = pd.read_sql(query, engine)
        print("\n Últimas 10 ventas:")
        print(result.to_string())
    except Exception as e:
        print(f"Error en verificación: {e}")
    
    engine.dispose()
    print("\n🔌 Conexión cerrada")

# ============================================
# EJECUCIÓN PRINCIPAL
# ============================================
if __name__ == "__main__":
    print("="*60)
    print(" INICIANDO PROCESO ETL COMPLETO")
    print("="*60)
    
    # Ejecutar ETL
    df = extract()
    if df is not None:
        entities = transform(df)
        if entities:
            load(entities)
            print("\n" + "="*60)
            print(" PROCESO ETL COMPLETADO EXITOSAMENTE")
            print("="*60)