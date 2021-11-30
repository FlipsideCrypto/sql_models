{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', METRIC_DATE, currency)",
    tags = ['snowflake', 'terra', 'console']
) }}

SELECT day as metric_date, 
       currency,
       total_vest,
       percent_vest
FROM
(WITH date_range as
    (SELECT day
   FROM {{ ref('redshift__days') }}) 
SELECT day,
      'luna' as currency, 
      sum(vesting_amount) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_vest,
      total_vest/180094333 as percent_vest 
FROM(
SELECT date_trunc('day', vesting_date) AS metric_date, sum(vesting_amount) AS vesting_amount
      FROM {{ ref('gold__terra_vesting_schedule') }} 
            WHERE vesting_currency = 'luna'
      GROUP BY metric_date)sq
right OUTER join date_range
  ON metric_date = day

UNION

SELECT day,
      'sdr' as currency, 
      sum(vesting_amount) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_vest,
      total_vest/1002579718 as percent_vest 
FROM(
SELECT date_trunc('day', vesting_date) AS metric_date, sum(vesting_amount) AS vesting_amount
      FROM {{ ref('gold__terra_vesting_schedule') }} 
      WHERE vesting_currency = 'sdr'
      GROUP BY day)sq
right OUTER join date_range
  ON metric_date = day)si
WHERE day <= getdate() AND day >= '2019-05-01'::date
ORDER BY metric_date DESC