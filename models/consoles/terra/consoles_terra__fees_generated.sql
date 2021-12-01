{{ config(
  materialized = 'view',
  unique_key = 'BLOCK_DATE',
  tags = ['snowflake', 'terra', 'console']
) }}

WITH rawfees AS (

  SELECT
    block_timestamp :: DATE AS metric_date,
    CASE
      WHEN chain_id = 'columbus-5' THEN UPPER(SUBSTRING(fee [0] :amount [0] :denom :: STRING, 2, 2))
      ELSE UPPER(SUBSTRING(fee [0] :denom :: STRING, 2, 2))
    END AS fee_denom,
    SUM(
      CASE
        WHEN chain_id = 'columbus-5' THEN fee [0] :amount [0] :amount
        ELSE fee [0] :amount
      END
    ) / pow(
      10,
      6
    ) AS amount
  FROM
    {{ ref('terra__transactions') }}
  WHERE
    fee_denom IS NOT NULL
    AND block_timestamp :: DATE >= CURRENT_DATE - 90
  GROUP BY
    metric_date,
    fee_denom
  ORDER BY
    metric_date,
    fee_denom
),
prices AS (
  SELECT
    block_timestamp :: DATE AS metric_date,
    symbol,
    SUBSTRING(
      symbol,
      1,
      2
    ) AS fee_denom,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    block_timestamp :: DATE > CURRENT_DATE - 90
    AND symbol IN (
      'UST',
      'SDT',
      'AUT',
      'CAT',
      'EUT',
      'JPT',
      'KRT',
      'LUNA',
      'MNT'
    )
  GROUP BY
    metric_date,
    symbol,
    fee_denom
)
SELECT
  r.metric_date AS block_date,
  SUM(
    amount * price
  ) AS amount
FROM
  rawfees r
  JOIN prices p
  ON r.metric_date = p.metric_date
  AND r.fee_denom = p.fee_denom
GROUP BY
  block_date
ORDER BY
  block_date DESC,
  amount
