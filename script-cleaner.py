"""
ETL COMPLETO - SISTEMA DE VENTAS
Proceso documentado de carga desde Excel a PostgreSQL (Staging → Analytics)

Autor: [Tu nombre]
Fecha: 2025
Base de datos: PostgreSQL en Docker
Modelo: Copo de Nieve
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import numpy as np


# ============================================
# CONFIGURACIÓN
# ============================================
print("="*70)
print("INICIO DEL PROCESO ETL")
print("="*70)

# Conexión a PostgreSQL
DB_CONNECTION = 'postgresql://bi_user:bi_password_123@localhost:5432/bi_database'
engine = create_engine(DB_CONNECTION)

# Rutas de los archivos Excel (ajusta según tu ubicación)
EXCEL_FILES = {
    'productos': 'data/listado_de_productos.xlsx',
    'ventas': 'data/listado_de_ventas.xlsx',
    'clientes': 'data/report_de_cliente.xlsx',
    'detalle': 'data/detalle_de_venta.xlsx'
}

# ============================================
# FUNCIONES DE LIMPIEZA
# ============================================

def limpiar_texto(texto):
    """Limpia y normaliza texto: trim, uppercase"""
    if pd.isna(texto) or texto == '':
        return None
    return str(texto).strip().upper()

def limpiar_numero(valor):
    """Convierte texto a número decimal"""
    if pd.isna(valor) or valor == '':
        return 0
    if isinstance(valor, (int, float)):
        return float(valor)
    # Remover espacios, comas y convertir punto decimal
    valor_limpio = str(valor).replace(',', '.').replace(' ', '')
    try:
        return float(valor_limpio)
    except:
        return 0

def limpiar_fecha(fecha):
    """Convierte texto a fecha"""
    if pd.isna(fecha):
        return None
    try:
        return pd.to_datetime(fecha)
    except:
        return None

def registrar_paso(paso, descripcion, registros_procesados=0, registros_validos=0):
    """Registra cada paso del ETL para documentación"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    descartados = registros_procesados - registros_validos if registros_procesados > 0 else 0
    
    print(f"\n{'='*70}")
    print(f"[{timestamp}] PASO {paso}: {descripcion}")
    if registros_procesados > 0:
        print(f"├─ Registros procesados: {registros_procesados}")
        print(f"├─ Registros válidos: {registros_validos}")
        print(f"└─ Registros descartados: {descartados}")
    print(f"{'='*70}")

# ============================================
# FASE 1: EXTRACCIÓN - CARGAR EXCELS A STAGING
# ============================================

registrar_paso(1, "EXTRACCIÓN - Cargando Excels crudos a Staging")

# 1.1 - Productos
print("\n📦 Cargando productos...")
df_productos = pd.read_excel(EXCEL_FILES['productos'])
df_productos['fecha_carga'] = datetime.now()
df_productos.to_sql('raw_productos', engine, schema='staging', 
                    if_exists='replace', index=False)
print(f"✅ {len(df_productos)} productos cargados a staging.raw_productos")

# 1.2 - Ventas
print("\n🛒 Cargando listado de ventas...")
df_ventas = pd.read_excel(EXCEL_FILES['ventas'])
df_ventas['fecha_carga'] = datetime.now()
df_ventas.to_sql('raw_ventas', engine, schema='staging', 
                 if_exists='replace', index=False)
print(f"✅ {len(df_ventas)} ventas cargadas a staging.raw_ventas")

# 1.3 - Clientes
print("\n👥 Cargando clientes...")
df_clientes = pd.read_excel(EXCEL_FILES['clientes'])
df_clientes['fecha_carga'] = datetime.now()
df_clientes.to_sql('raw_clientes', engine, schema='staging', 
                   if_exists='replace', index=False)
print(f"✅ {len(df_clientes)} clientes cargados a staging.raw_clientes")

# 1.4 - Detalle de venta
print("\n📋 Cargando detalle de ventas...")
df_detalle = pd.read_excel(EXCEL_FILES['detalle'])
df_detalle['fecha_carga'] = datetime.now()
df_detalle.to_sql('raw_detalle_venta', engine, schema='staging', 
                  if_exists='replace', index=False)
print(f"✅ {len(df_detalle)} detalles cargados a staging.raw_detalle_venta")

# ============================================
# FASE 2: TRANSFORMACIÓN - LIMPIEZA DE DATOS
# ============================================

registrar_paso(2, "TRANSFORMACIÓN - Limpieza y validación de datos")

# 2.1 - Limpiar Productos
print("\n🧹 Limpiando productos...")
df_prod_clean = df_productos.copy()
total_productos = len(df_prod_clean)

# Solo columnas amarillas: Código, Nombre, Marca, Categorias, Unidad
df_prod_clean['Código'] = df_prod_clean['Código'].apply(limpiar_texto)
df_prod_clean['Nombre'] = df_prod_clean['Nombre'].apply(limpiar_texto)
df_prod_clean['Marca'] = df_prod_clean['Marca'].apply(limpiar_texto)
df_prod_clean['Categorias'] = df_prod_clean['Categorias'].apply(limpiar_texto)
df_prod_clean['Unidad'] = df_prod_clean['Unidad'].apply(limpiar_texto)

# Filtrar registros válidos (deben tener al menos código y nombre)
df_prod_clean = df_prod_clean.dropna(subset=['Código', 'Nombre'])
registrar_paso("2.1", "Productos limpiados", total_productos, len(df_prod_clean))

# 2.2 - Limpiar Clientes
print("\n🧹 Limpiando clientes...")
df_cli_clean = df_clientes.copy()
total_clientes = len(df_cli_clean)

# Solo columnas amarillas: Numero de documento, Nombres, Apellidos
df_cli_clean['Numero de documento'] = df_cli_clean['Numero de documento'].astype(str).str.strip()
df_cli_clean['Nombres'] = df_cli_clean['Nombres'].apply(limpiar_texto)
df_cli_clean['Apellidos'] = df_cli_clean['Apellidos'].apply(limpiar_texto)
df_cli_clean['Razon Social.'] = df_cli_clean['Razon Social.'].apply(limpiar_texto)

# Filtrar registros válidos (deben tener documento)
df_cli_clean = df_cli_clean.dropna(subset=['Numero de documento'])

# Lógica de nombre:
# 1. Si tiene Nombres, usar Nombres
# 2. Si no tiene Nombres pero tiene Razon Social, usar Razon Social
# 3. Si no tiene ni Nombres ni Razon Social, usar "CLIENTE SIN NOMBRE"
df_cli_clean['Nombres'] = df_cli_clean.apply(
    lambda x: x['Nombres'] if pd.notna(x['Nombres']) else (
        x['Razon Social.'] if pd.notna(x['Razon Social.']) else 'CLIENTE SIN NOMBRE'
    ),
    axis=1
)

registrar_paso("2.2", "Clientes limpiados", total_clientes, len(df_cli_clean))

# 2.3 - Limpiar Detalle de Venta
print("\n🧹 Limpiando detalle de ventas...")
df_det_clean = df_detalle.copy()
total_detalle = len(df_det_clean)

# Columnas amarillas del detalle
df_det_clean['Unidad'] = df_det_clean['Unidad'].apply(limpiar_texto)
df_det_clean['Cantidad'] = df_det_clean['Cantidad'].apply(limpiar_numero)
df_det_clean['Total'] = df_det_clean['Total'].apply(limpiar_numero)
df_det_clean['Codigo SKU'] = df_det_clean['Codigo SKU'].astype(str).str.strip()
df_det_clean['Marca'] = df_det_clean['Marca'].apply(limpiar_texto)

# Columnas de cabecera
df_det_clean['Empleado Nombre'] = df_det_clean['Empleado Nombre'].apply(limpiar_texto)
df_det_clean['Cliente Nombre'] = df_det_clean['Cliente Nombre'].apply(limpiar_texto)
df_det_clean['Cliente Doc.'] = df_det_clean['Cliente Doc.'].astype(str).str.strip()
df_det_clean['#-DOC'] = df_det_clean['#-DOC'].astype(str).str.strip()
df_det_clean['Fecha'] = df_det_clean['Fecha'].apply(limpiar_fecha)

# Filtrar registros válidos
df_det_clean = df_det_clean.dropna(subset=['#-DOC', 'Codigo SKU', 'Cantidad'])
df_det_clean = df_det_clean[df_det_clean['Cantidad'] > 0]
registrar_paso("2.3", "Detalle de ventas limpiado", total_detalle, len(df_det_clean))

# ============================================
# FASE 3: CARGA - POBLAR DIMENSIONES
# ============================================

registrar_paso(3, "CARGA - Poblando dimensiones del modelo copo de nieve")

# 3.1 - Cargar MARCAS (desde detalle de venta)
print("\n📦 Cargando dimensión: MARCAS")
marcas_detalle = df_det_clean['Marca'].dropna().unique()
marcas_unicas = list(marcas_detalle)

with engine.connect() as conn:
    for marca in marcas_unicas:
        conn.execute(text("""
            INSERT INTO analytics.marca (nombre)
            VALUES (:nombre)
            ON CONFLICT (nombre) DO NOTHING
        """), {"nombre": marca})
    conn.commit()

registrar_paso("3.1", "Marcas cargadas", len(marcas_unicas), len(marcas_unicas))

# 3.2 - Cargar CATEGORÍAS (desde detalle de venta)
print("\n📂 Cargando dimensión: CATEGORÍAS")
categorias_detalle = df_det_clean['Categoría'].dropna().unique()

with engine.connect() as conn:
    for cat in categorias_detalle:
        conn.execute(text("""
            INSERT INTO analytics.categoria (nombre)
            VALUES (:nombre)
            ON CONFLICT (nombre) DO NOTHING
        """), {"nombre": cat})
    conn.commit()

registrar_paso("3.2", "Categorías cargadas", len(categorias_detalle), len(categorias_detalle))

# 3.3 - Cargar PRODUCTOS (desde detalle de venta con Codigo SKU)
print("\n🏷️ Cargando dimensión: PRODUCTOS")

# Obtener productos únicos del detalle (usa Codigo SKU como PK)
productos_detalle = df_det_clean[['Codigo SKU', 'Nombre', 'Marca', 'Categoría']].drop_duplicates('Codigo SKU')
productos_detalle = productos_detalle.dropna(subset=['Codigo SKU'])

with engine.connect() as conn:
    for _, prod in productos_detalle.iterrows():
        # Obtener IDs de marca y categoría
        marca_id = None
        if pd.notna(prod['Marca']):
            marca_result = conn.execute(text("""
                SELECT cod_marca FROM analytics.marca WHERE nombre = :nombre
            """), {"nombre": prod['Marca']}).fetchone()
            if marca_result:
                marca_id = marca_result[0]
        
        cat_id = None
        if pd.notna(prod['Categoría']):
            cat_result = conn.execute(text("""
                SELECT cod_cat FROM analytics.categoria WHERE nombre = :nombre
            """), {"nombre": prod['Categoría']}).fetchone()
            if cat_result:
                cat_id = cat_result[0]
        
        # Insertar producto usando Codigo SKU como PK
        conn.execute(text("""
            INSERT INTO analytics.producto 
            (cod_producto, nombre, cod_marca, cod_categoria)
            VALUES (:cod, :nombre, :marca, :cat)
            ON CONFLICT (cod_producto) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                cod_marca = EXCLUDED.cod_marca,
                cod_categoria = EXCLUDED.cod_categoria
        """), {
            "cod": prod['Codigo SKU'],
            "nombre": prod['Nombre'] if pd.notna(prod['Nombre']) else 'SIN NOMBRE',
            "marca": marca_id,
            "cat": cat_id
        })
    conn.commit()

registrar_paso("3.3", "Productos cargados", len(productos_detalle), len(productos_detalle))

# 3.4 - Cargar CLIENTES (solo doc, nombre, apellidos)
print("\n👥 Cargando dimensión: CLIENTES")
with engine.connect() as conn:
    for _, cli in df_cli_clean.iterrows():
        conn.execute(text("""
            INSERT INTO analytics.cliente 
            (doc_cliente, nombre, apellidos)
            VALUES (:doc, :nombre, :apellidos)
            ON CONFLICT (doc_cliente) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                apellidos = EXCLUDED.apellidos
        """), {
            "doc": cli['Numero de documento'],
            "nombre": cli['Nombres'],
            "apellidos": cli['Apellidos'] if pd.notna(cli['Apellidos']) else None
        })
    conn.commit()

registrar_paso("3.4", "Clientes cargados", len(df_cli_clean), len(df_cli_clean))

# 3.5 - Cargar EMPLEADOS
print("\n👷 Cargando dimensión: EMPLEADOS")
empleados_unicos = df_det_clean['Empleado Nombre'].dropna().unique()

with engine.connect() as conn:
    for emp in empleados_unicos:
        conn.execute(text("""
            INSERT INTO analytics.empleado (nombre)
            VALUES (:nombre)
            ON CONFLICT (nombre) DO NOTHING
        """), {"nombre": emp})
    conn.commit()

registrar_paso("3.5", "Empleados cargados", len(empleados_unicos), len(empleados_unicos))

# 3.6 - Cargar TIEMPO
print("\n📅 Cargando dimensión: TIEMPO")
fechas_unicas = df_det_clean['Fecha'].dropna().dt.date.unique()

with engine.connect() as conn:
    for fecha in fechas_unicas:
        fecha_dt = pd.to_datetime(fecha)
        conn.execute(text("""
            INSERT INTO analytics.tiempo 
            (fecha, anio, mes, dia, trimestre, nombre_mes, dia_semana, nombre_dia_semana)
            VALUES (:fecha, :anio, :mes, :dia, :trim, :nom_mes, :dia_sem, :nom_dia)
            ON CONFLICT (fecha) DO NOTHING
        """), {
            "fecha": fecha,
            "anio": fecha_dt.year,
            "mes": fecha_dt.month,
            "dia": fecha_dt.day,
            "trim": (fecha_dt.month - 1) // 3 + 1,
            "nom_mes": fecha_dt.strftime('%B'),
            "dia_sem": fecha_dt.weekday(),
            "nom_dia": fecha_dt.strftime('%A')
        })
    conn.commit()

registrar_paso("3.6", "Fechas cargadas", len(fechas_unicas), len(fechas_unicas))

# ============================================
# FASE 4: CARGA - TABLAS DE HECHOS
# ============================================

registrar_paso(4, "CARGA - Poblando tablas de hechos (Ventas y Detalle)")

# 4.1 - Cargar VENTAS (Cabecera - simplificado)
print("\n🛒 Cargando tabla de hechos: VENTAS")

# Agrupar por #-DOC para obtener totales y datos de cabecera
ventas_agrupadas = df_det_clean.groupby('#-DOC').agg({
    'Total': 'sum',
    'Cliente Doc.': 'first',
    'Empleado Nombre': 'first',
    'Fecha': 'first'
}).reset_index()

ventas_cargadas = 0
ventas_descartadas = 0

with engine.connect() as conn:
    for _, venta in ventas_agrupadas.iterrows():
        try:
            # Obtener ID de empleado
            emp_result = conn.execute(text("""
                SELECT cod_empleado FROM analytics.empleado WHERE nombre = :nombre
            """), {"nombre": venta['Empleado Nombre']}).fetchone()
            
            # Obtener ID de tiempo
            tiempo_result = conn.execute(text("""
                SELECT cod_time FROM analytics.tiempo WHERE fecha = :fecha
            """), {"fecha": venta['Fecha'].date()}).fetchone()
            
            if emp_result and tiempo_result:
                conn.execute(text("""
                    INSERT INTO analytics.ventas 
                    (cod_bolt, total, doc_cliente, doc_empleado, cod_time, fecha_venta)
                    VALUES (:bolt, :total, :cliente, :emp, :tiempo, :fecha)
                    ON CONFLICT (cod_bolt) DO NOTHING
                """), {
                    "bolt": venta['#-DOC'],
                    "total": round(venta['Total'], 2),
                    "cliente": venta['Cliente Doc.'],
                    "emp": emp_result[0],
                    "tiempo": tiempo_result[0],
                    "fecha": venta['Fecha']
                })
                ventas_cargadas += 1
            else:
                ventas_descartadas += 1
        except Exception as e:
            print(f"⚠️ Error en venta {venta['#-DOC']}: {str(e)}")
            ventas_descartadas += 1
    
    conn.commit()

registrar_paso("4.1", "Ventas cargadas", len(ventas_agrupadas), ventas_cargadas)

# 4.2 - Cargar DETALLE DE VENTAS (simplificado)
print("\n📋 Cargando tabla de hechos: DETALLE DE VENTAS")

# Calcular precio unitario
df_det_clean['precio_unitario'] = df_det_clean['Total'] / df_det_clean['Cantidad']
df_det_clean['precio_unitario'] = df_det_clean['precio_unitario'].replace([np.inf, -np.inf], 0)

detalles_cargados = 0
detalles_descartados = 0

with engine.connect() as conn:
    for _, det in df_det_clean.iterrows():
        try:
            conn.execute(text("""
                INSERT INTO analytics.detalle_venta 
                (cod_producto, cod_bolt, unidad, cantidad, precio_unitario, subtotal)
                VALUES (:prod, :bolt, :unidad, :cant, :precio, :subtotal)
            """), {
                "prod": det['Codigo SKU'],
                "bolt": det['#-DOC'],
                "unidad": det['Unidad'],
                "cant": round(det['Cantidad'], 2),
                "precio": round(det['precio_unitario'], 2),
                "subtotal": round(det['Total'], 2)
            })
            detalles_cargados += 1
        except Exception as e:
            print(f"⚠️ Error en detalle: {str(e)}")
            detalles_descartados += 1
    
    conn.commit()

registrar_paso("4.2", "Detalles de venta cargados", len(df_det_clean), detalles_cargados)

# ============================================
# RESUMEN FINAL Y VALIDACIÓN
# ============================================

print("\n" + "="*70)
print("✨ ETL COMPLETADO EXITOSAMENTE")
print("="*70)

with engine.connect() as conn:
    stats = {
        'STAGING - Productos crudos': conn.execute(text("SELECT COUNT(*) FROM staging.raw_productos")).scalar(),
        'STAGING - Ventas crudas': conn.execute(text("SELECT COUNT(*) FROM staging.raw_ventas")).scalar(),
        'STAGING - Clientes crudos': conn.execute(text("SELECT COUNT(*) FROM staging.raw_clientes")).scalar(),
        'STAGING - Detalle crudo': conn.execute(text("SELECT COUNT(*) FROM staging.raw_detalle_venta")).scalar(),
        '---': '---',
        'ANALYTICS - Marcas': conn.execute(text("SELECT COUNT(*) FROM analytics.marca")).scalar(),
        'ANALYTICS - Categorías': conn.execute(text("SELECT COUNT(*) FROM analytics.categoria")).scalar(),
        'ANALYTICS - Productos': conn.execute(text("SELECT COUNT(*) FROM analytics.producto")).scalar(),
        'ANALYTICS - Clientes': conn.execute(text("SELECT COUNT(*) FROM analytics.cliente")).scalar(),
        'ANALYTICS - Empleados': conn.execute(text("SELECT COUNT(*) FROM analytics.empleado")).scalar(),
        'ANALYTICS - Fechas': conn.execute(text("SELECT COUNT(*) FROM analytics.tiempo")).scalar(),
        'ANALYTICS - Ventas': conn.execute(text("SELECT COUNT(*) FROM analytics.ventas")).scalar(),
        'ANALYTICS - Detalles': conn.execute(text("SELECT COUNT(*) FROM analytics.detalle_venta")).scalar()
    }

print("\n📊 RESUMEN DE REGISTROS:")
for tabla, count in stats.items():
    if tabla == '---':
        print(f"\n{'─'*70}")
    else:
        print(f"  {tabla:.<50} {count:>6}")

print("\n" + "="*70)
print("🎉 MODELO COPO DE NIEVE LISTO PARA ANÁLISIS BI")
print("="*70)
print(f"\n✅ Proceso completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n💡 Próximos pasos:")
print("  1. Verificar datos en pgAdmin")
print("  2. Crear vistas analíticas")
print("  3. Conectar herramienta BI (Power BI, Tableau, etc.)")
print("\n" + "="*70)