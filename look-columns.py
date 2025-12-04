import pandas as pd

EXCEL_FILES = {
    'productos': 'data/listado_de_productos.xlsx',
    'ventas': 'data/listado_de_ventas.xlsx',
    'clientes': 'data/report_de_cliente.xlsx',
    'detalle': 'data/detalle_de_venta.xlsx'
}

print("="*70)
print("ANÁLISIS DE COLUMNAS EN LOS EXCELS")
print("="*70)

for nombre, ruta in EXCEL_FILES.items():
    print(f"\n📄 {nombre.upper()} ({ruta})")
    print("-"*70)
    df = pd.read_excel(ruta)
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2}. {col}")
    print(f"\n  Total: {len(df.columns)} columnas | {len(df)} filas")