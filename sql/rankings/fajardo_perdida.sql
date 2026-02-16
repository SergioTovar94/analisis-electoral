COPY (
    WITH votos_por_puesto_evento AS (
        SELECT
            cod_departamento,
            departamento,
            cod_municipio,
            municipio,
            cod_zona,
            cod_puesto,
            puesto_votacion,
            dataset,
            SUM(votos_totales) AS votos
        FROM resultado_puesto
        WHERE candidato ILIKE '%FAJARDO%'
          AND dataset IN (
              '2022_CONSULTA_CENTRO_ESPERANZA',
              'MMV_NACIONAL_PRESIDENTE_2022_1v'
          )
        GROUP BY
            cod_departamento,
            departamento,
            cod_municipio,
            municipio,
            cod_zona,
            cod_puesto,
            puesto_votacion,
            dataset
    ),
    puestos_con_votos_ambos_eventos AS (
        SELECT cod_puesto
        FROM votos_por_puesto_evento
        WHERE votos > 0
        GROUP BY cod_puesto
        HAVING COUNT(DISTINCT dataset) = 2
    )
    SELECT
        'FAJARDO' AS candidato,
        v.cod_departamento,
        v.departamento,
        v.cod_municipio,
        v.municipio,
        v.cod_zona,
        v.cod_puesto,
        v.puesto_votacion,

        SUM(
            CASE
                WHEN v.dataset = '2022_CONSULTA_CENTRO_ESPERANZA'
                THEN v.votos ELSE 0
            END
        )
        -
        SUM(
            CASE
                WHEN v.dataset = 'MMV_NACIONAL_PRESIDENTE_2022_1v'
                THEN v.votos ELSE 0
            END
        ) AS votos_perdidos,

        SUM(
            CASE
                WHEN v.dataset = '2022_CONSULTA_CENTRO_ESPERANZA'
                THEN v.votos ELSE 0
            END
        ) AS votos_consulta_2022,

        SUM(
            CASE
                WHEN v.dataset = 'MMV_NACIONAL_PRESIDENTE_2022_1v'
                THEN v.votos ELSE 0
            END
        ) AS votos_1v_2022

    FROM votos_por_puesto_evento v
    JOIN puestos_con_votos_ambos_eventos p
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
) TO '/data/rankings/fajardo_perdida.csv'
WITH CSV HEADER;
