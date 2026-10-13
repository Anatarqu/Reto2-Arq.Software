-- =====================================================================
-- Esquema mínimo de DB Core para el experimento (subconjunto del modelo
-- canónico comercial + seguridad/disponibilidad).
-- =====================================================================
CREATE TABLE existencia (               -- ExistenciaEnBodega
    producto_id  INT NOT NULL,
    bodega_id    INT NOT NULL,
    inicial      INT NOT NULL,          -- stock sembrado (solo para verificar)
    disponible   INT NOT NULL CHECK (disponible >= 0),
    reservado    INT NOT NULL CHECK (reservado  >= 0),
    PRIMARY KEY (producto_id, bodega_id)
);

CREATE TABLE pedido (
    id          UUID PRIMARY KEY,
    vendedor_id INT  NOT NULL,
    tendero_id  INT  NOT NULL,
    estado      TEXT NOT NULL,
    epoch       INT  NOT NULL,          -- epoch del líder que lo escribió
    instancia   TEXT NOT NULL,
    creado_en   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE linea_pedido (
    pedido_id   UUID NOT NULL REFERENCES pedido(id),
    producto_id INT  NOT NULL,
    bodega_id   INT  NOT NULL,
    cantidad    INT  NOT NULL CHECK (cantidad > 0),
    PRIMARY KEY (pedido_id, producto_id, bodega_id)
);

CREATE TABLE reserva (
    pedido_id   UUID NOT NULL REFERENCES pedido(id),
    producto_id INT  NOT NULL,
    bodega_id   INT  NOT NULL,
    cantidad    INT  NOT NULL CHECK (cantidad > 0),
    PRIMARY KEY (pedido_id, producto_id, bodega_id)
);

CREATE TABLE registro_idempotencia (    -- RegistroIdempotencia
    idem_key   TEXT PRIMARY KEY,        -- UNIQUE = fuente de verdad anti-duplicados
    pedido_id  UUID NOT NULL,
    creado_en  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Fencing: el epoch mínimo que la BD acepta para escribir.
CREATE TABLE fencing (
    id    INT PRIMARY KEY CHECK (id = 1),
    epoch INT NOT NULL
);
INSERT INTO fencing VALUES (1, 0);

-- Historia de epochs (cuándo empezó cada uno). Sirve para auditar después.
CREATE TABLE epoca (
    epoch INT PRIMARY KEY,
    lider TEXT NOT NULL,
    desde TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Línea de tiempo del experimento (promotor, runner...).
CREATE TABLE eventos (
    id         BIGSERIAL PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    componente TEXT NOT NULL,
    tipo       TEXT NOT NULL,
    detalle    JSONB NOT NULL DEFAULT '{}'
);
