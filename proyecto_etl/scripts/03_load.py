"""
Módulo de Carga: Inserta los datos normalizados en MySQL
"""

import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG

class MySQLLoader:
    """
    Clase para cargar datos en MySQL
    """
    
    def __init__(self, config):
        self.config = config
        self.engine = None
    
    def connect(self):
        """
        Establece conexión con MySQL
        """
        try:
            # Crear URL de conexión sin base de datos específica
            db_url = f"mysql+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}"
            self.engine = create_engine(db_url)
            print(" Conexión a MySQL establecida")
            return True
        except Exception as e:
            print(f" Error de conexión: {e}")
            return False
    
    def create_database(self):
        """
        Crea la base de datos si no existe
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}"))
                conn.commit()
            print(f" Base de datos '{self.config['database']}' creada/verificada")
            
            # Reconectar a la base de datos específica
            db_url_full = f"mysql+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            self.engine = create_engine(db_url_full)
            return True
        except Exception as e:
            print(f" Error al crear base de datos: {e}")
            return False
    
    def create_tables(self):
        """
        Crea las tablas con relaciones
        """
        
        create_tables_sql = """
        -- Tabla VENDEDORES
        CREATE TABLE IF NOT EXISTS vendedores (
            id_vendedor INT NOT NULL AUTO_INCREMENT,
            nombre VARCHAR(100) NOT NULL,
            PRIMARY KEY (id_vendedor)
        );
        
        -- Tabla PROPIEDADES
        CREATE TABLE IF NOT EXISTS propiedades (
            referencia VARCHAR(50) NOT NULL,
            fecha_alta DATE NOT NULL,
            tipo VARCHAR(50),
            operacion VARCHAR(50),
            provincia VARCHAR(100),
            superficie DECIMAL(10,2),
            precio_venta DECIMAL(12,2),
            PRIMARY KEY (referencia)
        );
        
        -- Tabla VENTAS
        CREATE TABLE IF NOT EXISTS ventas (
            id_venta INT NOT NULL AUTO_INCREMENT,
            referencia VARCHAR(50) NOT NULL,
            fecha_venta DATE NOT NULL,
            id_vendedor INT,
            PRIMARY KEY (id_venta),
            FOREIGN KEY (referencia) REFERENCES propiedades(referencia) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (id_vendedor) REFERENCES vendedores(id_vendedor) ON DELETE SET NULL
        );
        
        -- Tabla DETALLE_VENTA (para mantener consistencia, aunque sea 1:1)
        CREATE TABLE IF NOT EXISTS detalle_venta (
            id_venta INT NOT NULL,
            referencia VARCHAR(50) NOT NULL,
            PRIMARY KEY (id_venta),
            FOREIGN KEY (id_venta) REFERENCES ventas(id_venta) ON DELETE CASCADE,
            FOREIGN KEY (referencia) REFERENCES propiedades(referencia) ON DELETE CASCADE
        );
        """
        
        try:
            for statement in create_tables_sql.split(';'):
                if statement.strip():
                    with self.engine.begin() as conn:
                        conn.execute(text(statement))
            print(" Tablas creadas correctamente")
            return True
        except Exception as e:
            print(f" Error al crear tablas: {e}")
            return False
    
    def load_data(self, entities):
        """
        Carga los datos en las tablas
        """
        print("\n" + "="*50)
        print("CARGANDO DATOS EN MYSQL")
        print("="*50)
        
        # Orden de carga respetando las relaciones
        load_order = ['vendedores', 'propiedades', 'ventas', 'detalle_venta']
        
        for table_name in load_order:
            if table_name in entities:
                df = entities[table_name]
                try:
                    df.to_sql(table_name, self.engine, if_exists='append', index=False)
                    print(f" Datos cargados en '{table_name}': {len(df)} registros")
                except Exception as e:
                    print(f" Error al cargar '{table_name}': {e}")
                    print(f"   Detalle: {df.head()}")
    
    def verify_data(self):
        """
        Verifica que los datos se cargaron correctamente
        """
        print("\n" + "="*60)
        print("VERIFICACIÓN DE DATOS CARGADOS")
        print("="*60)
        
        # Consulta de verificación completa
        query = """
        SELECT 
            v.id_venta,
            v.fecha_venta,
            p.referencia,
            p.tipo,
            p.operacion,
            p.provincia,
            p.superficie,
            p.precio_venta,
            ve.nombre AS vendedor
        FROM ventas v
        JOIN propiedades p ON v.referencia = p.referencia
        LEFT JOIN vendedores ve ON v.id_vendedor = ve.id_vendedor
        ORDER BY v.fecha_venta DESC
        LIMIT 10;
        """
        
        try:
            df_result = pd.read_sql(query, self.engine)
            print("\n Últimas 10 ventas registradas:")
            print(df_result.to_string())
            
            # Estadísticas adicionales
            stats_query = """
            SELECT 
                COUNT(*) AS total_ventas,
                COUNT(DISTINCT referencia) AS propiedades_vendidas,
                MIN(fecha_venta) AS primera_venta,
                MAX(fecha_venta) AS ultima_venta,
                AVG(p.precio_venta) AS precio_promedio
            FROM ventas v
            JOIN propiedades p ON v.referencia = p.referencia;
            """
            
            df_stats = pd.read_sql(stats_query, self.engine)
            print("\n Estadísticas generales:")
            print(df_stats.to_string())
            
        except Exception as e:
            print(f" Error al verificar datos: {e}")
    
    def close(self):
        """
        Cierra la conexión
        """
        if self.engine:
            self.engine.dispose()
        print("\n🔌 Conexión cerrada")

def load_main(entities):
    """
    Función principal de carga
    """
    loader = MySQLLoader(DB_CONFIG)
    
    # Conectar y crear base de datos
    if not loader.connect():
        return False
    
    if not loader.create_database():
        return False
    
    # Crear tablas
    if not loader.create_tables():
        return False
    
    # Cargar datos
    loader.load_data(entities)
    
    # Verificar
    loader.verify_data()
    
    # Cerrar conexión
    loader.close()
    
    return True

if __name__ == "__main__":
    print("Este script debe ser ejecutado desde etl_completo.py")