COPY(
    SELECT
        'EN BLANCO' AS candidato,
        v.anio_eleccion,
        v.cod_departamento,
        v.departamento,
        v.cod_municipio,
        v.municipio,
        v.cod_zona,
        v.cod_puesto,
        v.puesto_votacion,
        SUM(v.votos_totales) AS votos_totales
    FROM resultado_puesto v
    WHERE v.candidato ILIKE '%EN BLANCO%'
      AND v.anio_eleccion IN (2018, 2022)
    GROUP BY
        v.anio_eleccion,
        v.cod_departamento,
        v.departamento,
        v.cod_municipio,
        v.municipio,
        v.cod_zona,
        v.cod_puesto,
        v.puesto_votacion
    ORDER BY votos_totales DESC
) TO '/data/rankings/votos_blanco.csv'
WITH CSV HEADER;
