-- =========================================================
-- DIMENSIONES GEOGRÁFICAS
-- =========================================================

CREATE TABLE departamento (
    id SERIAL PRIMARY KEY,
    nombre TEXT NOT NULL,
    codigo_dane TEXT UNIQUE
);

CREATE TABLE municipio (
    id SERIAL PRIMARY KEY,
    departamento_id INT REFERENCES departamento(id),
    nombre TEXT NOT NULL,
    codigo_dane TEXT UNIQUE
);

CREATE TABLE puesto_votacion (
    id SERIAL PRIMARY KEY,
    municipio_id INT REFERENCES municipio(id),
    codigo_puesto TEXT,
    nombre TEXT NOT NULL
);

-- =========================================================
-- DIMENSIÓN ELECCIÓN (NORMALIZADA)
-- =========================================================

CREATE TABLE eleccion (
    id SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    tipo TEXT NOT NULL,
    dataset TEXT UNIQUE   -- identifica unívocamente el proceso electoral
);

-- =========================================================
-- RESULTADOS A NIVEL MESA (NORMALIZADO)
-- =========================================================

CREATE TABLE resultado_mesa (
    id BIGSERIAL PRIMARY KEY,
    eleccion_id INT REFERENCES eleccion(id),
    puesto_id INT REFERENCES puesto_votacion(id),
    mesa TEXT,
    cod_partido INTEGER,
    partido TEXT,
    cod_candidato TEXT,
    candidato TEXT,
    votos INT NOT NULL
);

-- =========================================================
-- RESULTADOS A NIVEL PUESTO (TABLA ANALÍTICA / FACT TABLE)
-- =========================================================
-- Tabla desnormalizada optimizada para análisis y rankings

CREATE TABLE resultado_puesto (
    anio_eleccion INTEGER NOT NULL,

    cod_departamento INTEGER,
    departamento TEXT,

    cod_municipio INTEGER,
    municipio TEXT,

    cod_zona INTEGER,
    cod_comuna INTEGER,
    comuna TEXT,

    cod_puesto INTEGER,
    puesto_votacion TEXT,

    mesa TEXT,

    cod_partido INTEGER,
    partido TEXT,

    cod_candidato INTEGER,
    candidato TEXT,

    votos_totales INTEGER NOT NULL,

    archivo_fuente TEXT,
    dataset TEXT NOT NULL
);

-- =========================================================
-- ÍNDICES PARA PERFORMANCE ANALÍTICA
-- =========================================================

CREATE INDEX idx_rp_anio
ON resultado_puesto (anio_eleccion);

CREATE INDEX idx_rp_dataset
ON resultado_puesto (dataset);

CREATE INDEX idx_rp_anio_dataset
ON resultado_puesto (anio_eleccion, dataset);

CREATE INDEX idx_rp_geo
ON resultado_puesto (cod_departamento, cod_municipio);

CREATE INDEX idx_rp_puesto
ON resultado_puesto (cod_puesto);

CREATE INDEX idx_rp_candidato
ON resultado_puesto (candidato);

CREATE INDEX idx_rp_candidato_dataset
ON resultado_puesto (candidato, dataset);
