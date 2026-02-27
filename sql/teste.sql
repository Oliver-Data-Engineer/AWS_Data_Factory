WITH parametro AS (
    SELECT date_parse('{anomesdia}', '%Y%m%d') AS data_referencia
),

limites AS (
    SELECT
        date_trunc('month', data_referencia)              AS data_inicio,
        date_add(
            'day',
            -1,
            date_add('month', 1, date_trunc('month', data_referencia))
        )                                                 AS data_fim
    FROM parametro
)

SELECT
    d                                           AS data,
    date_format(d, '%Y%m%d')                    AS anomesdia,
    day(d)                                      AS dia,
    week(d)                                     AS semana_ano,
    day_of_week(d)                              AS dia_semana,
    date_format(d, '%W')                        AS nome_dia,
    quarter(d)                                  AS trimestre,
    year(d)                                     AS ano,
    month(d)                                    AS mes
FROM limites
CROSS JOIN UNNEST(
    sequence(data_inicio, data_fim, interval '1' day)
) AS t(d)
ORDER BY data