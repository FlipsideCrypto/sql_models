{{ config(
  materialized = 'view',
  unique_key = 'block_date',
  tags = ['snowflake', 'terra', 'terraswap', 'console']
) }}

WITH prices AS (

  SELECT
    DATE(block_timestamp) block_date,
    currency,
    AVG(price_usd) avg_price_usd
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    block_timestamp > CURRENT_DATE - INTERVAL '90 days'
  GROUP BY
    block_date,
    currency
),
swaps AS (
  SELECT
    DATE_TRUNC(
      'week',
      m.block_timestamp
    ) block_week,
    DATE_TRUNC(
      'month',
      m.block_timestamp
    ) block_month,
    DATE(
      m.block_timestamp
    ) block_date,
    m.block_timestamp,
    m.tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :coins [0] :amount / pow(
      10,
      6
    ) offered_amount,
    msg_value :coins [0] :denom :: STRING offered_denom,
    (msg_value :coins [0] :amount / pow(10, 6)) * p.avg_price_usd swap_usd_value,
    SUBSTR(C.address, 11, len(C.address)) AS contract_label
  FROM
    {{ ref('terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('terra__labels') }} C
    ON msg_value :contract = C.address
    JOIN prices p
    ON DATE(
      m.block_timestamp
    ) = p.block_date
    AND msg_value :coins [0] :denom :: STRING = p.currency
  WHERE
    msg_value :execute_msg :swap IS NOT NULL
    AND m.block_timestamp >= CURRENT_DATE - 90
    AND m.tx_status = 'SUCCEEDED'
  ORDER BY
    m.block_timestamp DESC
)
SELECT
  block_date,
  SUM(swap_usd_value) swap_value_usd
FROM
  swaps
GROUP BY
  block_date
ORDER BY
  block_date DESC
