\copy (
    SELECT
        'NULO' AS candidato,
        v.departamento,
        v.municipio,
        v.puesto_votacion,
        SUM(v.votos_totales) AS votos_nulos_totales,
        SUM(CASE WHEN v.anio_eleccion = 2018 THEN v.votos_totales ELSE 0 END) AS votos_2018,
        SUM(CASE WHEN v.anio_eleccion = 2022 THEN v.votos_totales ELSE 0 END) AS votos_2022
    FROM resultado_puesto v
    WHERE candidato ILIKE '%NULO%'
      AND anio_eleccion IN (2018, 2022)
    GROUP BY v.departamento, v.municipio, v.puesto_votacion
    ORDER BY votos_nulos_totales DESC
) TO '/data/rankings/votos_nulos.csv' WITH CSV HEADER;
