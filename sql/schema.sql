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
    candidato TEXT,
    votos INT,
    votos_nulos INT,
    votos_blancos INT
);

CREATE TABLE resultado_puesto (
    puesto_id INT REFERENCES puesto_votacion(id),
    eleccion_id INT REFERENCES eleccion(id),
    candidato TEXT,
    votos_totales INT,
    votos_nulos INT,
    votos_blancos INT,
    PRIMARY KEY (puesto_id, eleccion_id, candidato)
);

CREATE INDEX idx_resultado_puesto_eleccion
ON resultado_puesto (eleccion_id);

CREATE INDEX idx_resultado_puesto_puesto
ON resultado_puesto (puesto_id);

CREATE INDEX idx_resultado_puesto_candidato
ON resultado_puesto (candidato);
