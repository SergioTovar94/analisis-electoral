\copy (
    WITH votos_por_puesto_anio AS (
        SELECT
            cod_puesto,
            puesto_votacion,
            municipio,
            departamento,
            anio_eleccion,
            SUM(votos_totales) AS votos
        FROM resultado_puesto
        WHERE candidato ILIKE '%Robledo%'
          AND anio_eleccion IN (2018, 2022)
        GROUP BY cod_puesto, puesto_votacion, municipio, departamento, anio_eleccion
    ),
    puestos_con_votos_ambos_anios AS (
        SELECT cod_puesto
        FROM votos_por_puesto_anio
        WHERE votos > 0
        GROUP BY cod_puesto
        HAVING COUNT(DISTINCT anio_eleccion) = 2
    )
    SELECT
        'Robledo' AS candidato,
        v.departamento,
        v.municipio,
        v.puesto_votacion,
        SUM(CASE WHEN v.anio_eleccion = 2018 THEN v.votos ELSE 0 END) 
          - SUM(CASE WHEN v.anio_eleccion = 2022 THEN v.votos ELSE 0 END) AS votos_perdidos,
        SUM(CASE WHEN v.anio_eleccion = 2018 THEN v.votos ELSE 0 END) AS votos_2018,
        SUM(CASE WHEN v.anio_eleccion = 2022 THEN v.votos ELSE 0 END) AS votos_2022
    FROM votos_por_puesto_anio v
    JOIN puestos_con_votos_ambos_anios p
      ON p.cod_puesto = v.cod_puesto
    GROUP BY v.departamento, v.municipio, v.puesto_votacion
    ORDER BY votos_perdidos DESC
) TO '/data/rankings/robledo_perdida.csv' WITH CSV HEADER;
