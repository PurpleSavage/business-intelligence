-- ============================================
-- CREAR SCHEMAS
-- ============================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================
-- SCHEMA: ANALYTICS (Modelo Copo de Nieve)
-- ============================================

-- Tabla: Marca
CREATE TABLE analytics.marca (
    cod_marca SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);

-- Tabla: Categoria
CREATE TABLE analytics.categoria (
    cod_cat SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);

-- Tabla: Producto
CREATE TABLE analytics.producto (
    cod_producto SERIAL PRIMARY KEY,
    cod_marca INTEGER REFERENCES analytics.marca(cod_marca),
    cod_categoria INTEGER REFERENCES analytics.categoria(cod_cat),
    nombre VARCHAR(200) NOT NULL,
    descripcion TEXT,
    precio_unitario DECIMAL(10,2)
);

-- Tabla: Cliente
CREATE TABLE analytics.cliente (
    doc_cliente VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    telefono VARCHAR(20),
    direccion TEXT
);

-- Tabla: Empleado
CREATE TABLE analytics.empleado (
    cod_empleado SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    cargo VARCHAR(100),
    fecha_ingreso DATE
);

-- Tabla: Tiempo (dimensión de tiempo)
CREATE TABLE analytics.tiempo (
    cod_time SERIAL PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER,
    nombre_mes VARCHAR(20),
    dia_semana INTEGER,
    nombre_dia_semana VARCHAR(20)
);

-- Tabla: Ventas (tabla de hechos)
CREATE TABLE analytics.ventas (
    cod_bolt SERIAL PRIMARY KEY,
    total DECIMAL(12,2) NOT NULL,
    doc_cliente VARCHAR(20) REFERENCES analytics.cliente(doc_cliente),
    doc_empleado INTEGER REFERENCES analytics.empleado(cod_empleado),
    cod_time INTEGER REFERENCES analytics.tiempo(cod_time),
    fecha_venta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: Detalle_venta
CREATE TABLE analytics.detalle_venta (
    cod_detalle_venta SERIAL PRIMARY KEY,
    cod_producto INTEGER REFERENCES analytics.producto(cod_producto),
    cod_bolt INTEGER REFERENCES analytics.ventas(cod_bolt),
    unidad VARCHAR(20),
    cantidad INTEGER NOT NULL,
    precio_unitario DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(12,2) NOT NULL
);





-- ============================================
-- SCHEMA: STAGING (Tablas para datos crudos)
-- ============================================

-- Aquí cargas tus Excels sin restricciones
CREATE TABLE staging.raw_ventas (
    id SERIAL PRIMARY KEY,
    fecha TEXT,
    cliente TEXT,
    empleado TEXT,
    producto TEXT,
    cantidad TEXT,
    precio TEXT,
    total TEXT,
    -- Agrega todas las columnas que vienen en tu Excel
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.raw_productos (
    id SERIAL PRIMARY KEY,
    codigo TEXT,
    nombre TEXT,
    marca TEXT,
    categoria TEXT,
    precio TEXT,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.raw_clientes (
    id SERIAL PRIMARY KEY,
    documento TEXT,
    nombre TEXT,
    email TEXT,
    telefono TEXT,
    direccion TEXT,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);




-- ============================================
-- ÍNDICES para mejor performance
-- ============================================

CREATE INDEX idx_producto_marca ON analytics.producto(cod_marca);
CREATE INDEX idx_producto_categoria ON analytics.producto(cod_categoria);
CREATE INDEX idx_ventas_cliente ON analytics.ventas(doc_cliente);
CREATE INDEX idx_ventas_empleado ON analytics.ventas(doc_empleado);
CREATE INDEX idx_ventas_tiempo ON analytics.ventas(cod_time);
CREATE INDEX idx_detalle_producto ON analytics.detalle_venta(cod_producto);
CREATE INDEX idx_detalle_venta ON analytics.detalle_venta(cod_bolt);
CREATE INDEX idx_tiempo_fecha ON analytics.tiempo(fecha);

-- ============================================
-- COMENTARIOS (documentación)
-- ============================================

COMMENT ON SCHEMA staging IS 'Área de staging para datos crudos desde Excel';
COMMENT ON SCHEMA analytics IS 'Modelo copo de nieve limpio para BI';