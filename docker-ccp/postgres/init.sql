-- ============================================================================
-- CCP (Compañía Comercializadora de Productos) - Schema & RLS Setup
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Rol de Aplicación para Conexiones de Microservicios (No Superusuario)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ccp_app_user') THEN
        CREATE ROLE ccp_app_user WITH LOGIN PASSWORD 'ccp_secure_pass_2026';
    END IF;
END
$$;

-- 2. Tablas del Dominio de Negocio
CREATE TABLE IF NOT EXISTS vendedores (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    pais VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clientes (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    pais VARCHAR(50) NOT NULL,
    vendedor_id INT NOT NULL REFERENCES vendedores(id),
    direccion VARCHAR(200),
    limite_credito NUMERIC(12,2) DEFAULT 5000.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bodegas (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    pais VARCHAR(50) NOT NULL,
    ciudad VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS productos (
    id SERIAL PRIMARY KEY,
    codigo_sku VARCHAR(50) UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    precio_unitario NUMERIC(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS inventario (
    id SERIAL PRIMARY KEY,
    bodega_id INT NOT NULL REFERENCES bodegas(id),
    producto_id INT NOT NULL REFERENCES productos(id),
    cantidad_disponible INT NOT NULL CHECK (cantidad_disponible >= 0),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_bodega_producto UNIQUE (bodega_id, producto_id)
);

CREATE TABLE IF NOT EXISTS pedidos (
    id SERIAL PRIMARY KEY,
    uuid_pedido UUID DEFAULT uuid_generate_v4(),
    vendedor_id INT NOT NULL REFERENCES vendedores(id),
    cliente_id INT NOT NULL REFERENCES clientes(id),
    bodega_id INT NOT NULL REFERENCES bodegas(id),
    total NUMERIC(12,2) NOT NULL,
    estado VARCHAR(30) DEFAULT 'CONFIRMADO',
    idempotency_key VARCHAR(100) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS detalles_pedido (
    id SERIAL PRIMARY KEY,
    pedido_id INT NOT NULL REFERENCES pedidos(id),
    producto_id INT NOT NULL REFERENCES productos(id),
    cantidad INT NOT NULL CHECK (cantidad > 0),
    precio_unitario NUMERIC(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    tipo_evento VARCHAR(50) NOT NULL,
    actor_id VARCHAR(50),
    ip_origen VARCHAR(45),
    codigo_http INT,
    descripcion TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Habilitar y Forzar Row-Level Security (RLS) para Aislamiento de Carteras (SEC-02)
ALTER TABLE clientes ENABLE ROW LEVEL SECURITY;
ALTER TABLE clientes FORCE ROW LEVEL SECURITY;

ALTER TABLE pedidos ENABLE ROW LEVEL SECURITY;
ALTER TABLE pedidos FORCE ROW LEVEL SECURITY;

-- Políticas RLS: Solo permite acceder a filas donde vendedor_id coincide con app.current_vendedor_id
DROP POLICY IF EXISTS rls_clientes_vendedor ON clientes;
CREATE POLICY rls_clientes_vendedor ON clientes
    FOR ALL
    TO ccp_app_user, PUBLIC
    USING (
        vendedor_id = NULLIF(current_setting('app.current_vendedor_id', true), '')::INT
    );

DROP POLICY IF EXISTS rls_pedidos_vendedor ON pedidos;
CREATE POLICY rls_pedidos_vendedor ON pedidos
    FOR ALL
    TO ccp_app_user, PUBLIC
    USING (
        vendedor_id = NULLIF(current_setting('app.current_vendedor_id', true), '')::INT
    );

-- 4. Otorgar Permisos al Rol de Aplicación
GRANT USAGE ON SCHEMA public TO ccp_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ccp_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ccp_app_user;

-- 5. Datos Iniciales (Seed Data para Pruebas y Experimentos)
INSERT INTO vendedores (id, nombre, pais, email) VALUES
(1, 'Carlos Perez (Vendedor Colombia)', 'Colombia', 'carlos.perez@ccp.com'),
(2, 'Maria Rodriguez (Vendedora Mexico)', 'Mexico', 'maria.rodriguez@ccp.com'),
(3, 'Juan Gomez (Vendedor Peru)', 'Peru', 'juan.gomez@ccp.com')
ON CONFLICT (id) DO NOTHING;

INSERT INTO clientes (id, nombre, pais, vendedor_id, direccion) VALUES
(101, 'Tienda Don Pedro', 'Colombia', 1, 'Calle 45 # 12-34, Bogota'),
(102, 'Supermercado Central', 'Colombia', 1, 'Carrera 7 # 80-20, Bogota'),
(201, 'Abarrotes El Chavo', 'Mexico', 2, 'Av. Reforma 123, CDMX'),
(202, 'MiniSuper Guadalajara', 'Mexico', 2, 'Av. Juarez 555, Guadalajara'),
(301, 'Bodega San Martin', 'Peru', 3, 'Av. Larco 888, Lima')
ON CONFLICT (id) DO NOTHING;

INSERT INTO bodegas (id, nombre, pais, ciudad) VALUES
(1, 'Bodega Central Bogota', 'Colombia', 'Bogota'),
(2, 'Bodega Norte CDMX', 'Mexico', 'Ciudad de Mexico'),
(3, 'Bodega Callao', 'Peru', 'Lima')
ON CONFLICT (id) DO NOTHING;

INSERT INTO productos (id, codigo_sku, nombre, precio_unitario) VALUES
(1, 'SKU-BEB-001', 'Gaseosa Cola 1.5L', 3500.00),
(2, 'SKU-SNK-002', 'Papas Fritas 150g', 2800.00),
(3, 'SKU-LAC-003', 'Leche Entera 1L', 4200.00)
ON CONFLICT (id) DO NOTHING;

-- Inventario inicial con cantidad limitada para probar contención concurrente (AD-R2)
INSERT INTO inventario (bodega_id, producto_id, cantidad_disponible) VALUES
(1, 1, 10),  -- Solo 10 unidades para probar contención masiva
(1, 2, 500),
(1, 3, 200),
(2, 1, 1000),
(3, 1, 800)
ON CONFLICT (bodega_id, producto_id) DO NOTHING;
