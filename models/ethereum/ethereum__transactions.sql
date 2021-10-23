{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'block_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'events', 'transactions']
) }}

WITH events AS (

  SELECT
    tx_hash,
    event_count,
    tx_fee
  FROM
    (
      SELECT
        *,
        COUNT(
          CASE
            WHEN log_index IS NOT NULL THEN 1
          END
        ) over(
          PARTITION BY tx_hash
        ) AS event_count,
        MAX(fee) over(
          PARTITION BY tx_hash
        ) AS tx_fee
      FROM
        {{ ref('silver_ethereum__ethereum_events') }}
      WHERE

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
  block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
)
GROUP BY
  1,
  2,
  3
),
txn AS (
  SELECT
    *
  FROM
    {{ ref('silver_ethereum__ethereum_transactions') }}
  WHERE

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
  block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
),
eth_prices AS (
  SELECT
    p.symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'prices'
    ) }}
    p
    JOIN {{ source(
      'shared',
      'cmc_assets'
    ) }} A
    ON p.asset_id = A.asset_id
  WHERE
    A.asset_id = 1027

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '2 days'
{% else %}
  AND recorded_at >= getdate() - INTERVAL '9 months'
{% endif %}
GROUP BY
  p.symbol,
  HOUR
)
SELECT
  t.block_timestamp,
  t.block_id AS block_id,
  t.tx_hash AS tx_id,
  tx_position,
  nonce,
  from_address,
  from_labels.l1_label AS from_label_type,
  from_labels.l2_label AS from_label_subtype,
  from_labels.project_name AS from_label,
  from_labels.address_name AS from_address_name,
  to_address,
  to_labels.l1_label AS to_label_type,
  to_labels.l2_label AS to_label_subtype,
  to_labels.project_name AS to_label,
  to_labels.address_name AS to_address_name,
  C.symbol,
  t.input_method AS function_signature,
  f.text_signature AS function_name,
  gas_price,
  gas_limit,
  gas_used,
  tx_fee,
  tx_fee * p.price AS fee_usd,
  CASE
    WHEN success = 1 THEN TRUE
    ELSE FALSE
  END AS success,
  event_count
FROM
  events e
  JOIN txn t
  ON e.tx_hash = t.tx_hash
  LEFT OUTER JOIN {{ source(
    'ethereum',
    'sha256_function_signatures'
  ) }}
  f
  ON t.input_method = f.hex_signature
  AND f.importance = 1
  LEFT OUTER JOIN {{ source(
    'ethereum',
    'ethereum_token_contracts'
  ) }} C
  ON t.to_address = C.contract_address
  LEFT OUTER JOIN eth_prices p
  ON DATE_TRUNC(
    'hour',
    t.block_timestamp
  ) = p.hour
  LEFT OUTER JOIN {{ source(
    'ethereum',
    'ethereum_address_labels'
  ) }} AS from_labels
  ON from_address = from_labels.address
  LEFT OUTER JOIN {{ source(
    'ethereum',
    'ethereum_address_labels'
  ) }} AS to_labels
  ON to_address = to_labels.address
