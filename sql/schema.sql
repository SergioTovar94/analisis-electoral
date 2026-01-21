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

CREATE TABLE eleccion (
    id SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    tipo TEXT NOT NULL
);

CREATE TABLE resultado_mesa (
    id BIGSERIAL PRIMARY KEY,
    eleccion_id INT REFERENCES eleccion(id),
    puesto_id INT REFERENCES puesto_votacion(id),
    mesa TEXT,
    cod_candidato TEXT,
    candidato TEXT,
    votos INT NOT NULL
);


CREATE TABLE resultado_puesto (
    anio_eleccion INTEGER NOT NULL,
    cod_departamento INTEGER,
    departamento TEXT,
    cod_municipio INTEGER,
    municipio TEXT,
    cod_puesto INTEGER,
    puesto_votacion TEXT,
    cod_candidato INTEGER,
    candidato TEXT,
    votos_totales INTEGER NOT NULL
);



CREATE INDEX idx_rp_anio
ON resultado_puesto (anio_eleccion);

CREATE INDEX idx_rp_geo
ON resultado_puesto (cod_departamento, cod_municipio);

CREATE INDEX idx_rp_puesto
ON resultado_puesto (cod_puesto);

CREATE INDEX idx_rp_candidato
ON resultado_puesto (cod_candidato);
