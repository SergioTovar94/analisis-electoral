COPY (
    WITH votos_por_puesto_anio AS (
        SELECT
            cod_departamento,
            departamento,
            cod_municipio,
            municipio,
            cod_zona,
            cod_puesto,
            puesto_votacion,
            anio_eleccion,
            SUM(votos_totales) AS votos
        FROM resultado_puesto
        WHERE candidato ILIKE '%YEILOR%'
          AND anio_eleccion IN (2022, 2023)
        GROUP BY
            cod_departamento,
            departamento,
            cod_municipio,
            municipio,
            cod_zona,
            cod_puesto,
            puesto_votacion,
            anio_eleccion
    ),
    puestos_con_votos_ambos_anios AS (
        SELECT cod_puesto
        FROM votos_por_puesto_anio
        WHERE votos > 0
        GROUP BY cod_puesto
        HAVING COUNT(DISTINCT anio_eleccion) = 2
    )
    SELECT
        'YEILOR' AS candidato,
        v.cod_departamento,
        v.departamento,
        v.cod_municipio,
        v.municipio,
        v.cod_zona,
        v.cod_puesto,
        v.puesto_votacion,

        SUM(
            CASE WHEN v.anio_eleccion = 2022
            THEN v.votos ELSE 0 END
        )
        -
        SUM(
            CASE WHEN v.anio_eleccion = 2023
            THEN v.votos ELSE 0 END
        ) AS votos_perdidos,

        SUM(
            CASE WHEN v.anio_eleccion = 2022
            THEN v.votos ELSE 0 END
        ) AS votos_2022,

        SUM(
            CASE WHEN v.anio_eleccion = 2023
            THEN v.votos ELSE 0 END
        ) AS votos_2023

    FROM votos_por_puesto_anio v
    JOIN puestos_con_votos_ambos_anios p
      ON p.cod_puesto = v.cod_puesto
    GROUP BY
        v.cod_departamento,
        v.departamento,
        v.cod_municipio,
        v.municipio,
        v.cod_zona,
        v.cod_puesto,
        v.puesto_votacion
    ORDER BY votos_perdidos DESC
) TO '/data/rankings/yeilor_perdida.csv'
WITH CSV HEADER;
