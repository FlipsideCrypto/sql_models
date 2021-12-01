{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', METRIC_DATE, currency)",
  tags = ['snowflake', 'terra', 'console']
) }}

SELECT
  DAY AS metric_date,
  currency,
  total_vest,
  percent_vest
FROM
  (
    WITH date_range AS (
      SELECT
        HOUR :: DATE AS DAY
      FROM
        {{ source(
          'shared',
          'hours'
        ) }}
      GROUP BY
        DAY
    )
    SELECT
      DAY,
      'luna' AS currency,
      SUM(vesting_amount) over (
        ORDER BY
          DAY rows BETWEEN unbounded preceding
          AND CURRENT ROW
      ) AS total_vest,
      total_vest / 180094333 AS percent_vest
    FROM(
        SELECT
          DATE_TRUNC(
            'day',
            vesting_date
          ) AS metric_date,
          SUM(vesting_amount) AS vesting_amount
        FROM
          {{ source(
            'gold',
            'terra_vesting_schedule'
          ) }}
        WHERE
          vesting_currency = 'luna'
        GROUP BY
          metric_date
      ) sq
      RIGHT OUTER JOIN date_range
      ON metric_date = DAY
    UNION
    SELECT
      DAY,
      'sdr' AS currency,
      SUM(vesting_amount) over (
        ORDER BY
          DAY rows BETWEEN unbounded preceding
          AND CURRENT ROW
      ) AS total_vest,
      total_vest / 1002579718 AS percent_vest
    FROM(
        SELECT
          DATE_TRUNC(
            'day',
            vesting_date
          ) AS metric_date,
          SUM(vesting_amount) AS vesting_amount
        FROM
          {{ source(
            'gold',
            'terra_vesting_schedule'
          ) }}
        WHERE
          vesting_currency = 'sdr'
        GROUP BY
          metric_date
      ) sq
      RIGHT OUTER JOIN date_range
      ON metric_date = DAY
  ) si
WHERE
  DAY <= getdate()
  AND DAY >= '2019-06-23' :: DATE
ORDER BY
  metric_date DESC
