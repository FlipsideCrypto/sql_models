{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'polygon', 'polygon_transactions_gold', 'address_labels']
) }}

WITH events AS (

  SELECT
    tx_id,
    SUM(
      CASE
        WHEN log_index IS NOT NULL THEN 1
        ELSE 0
      END
    ) AS event_count
  FROM
    {{ ref('silver_polygon__udm_events') }}

{% if is_incremental() -%}
WHERE
  block_timestamp :: DATE >= (
    SELECT
      MAX(
        block_timestamp :: DATE
      )
    FROM
      {{ this }}
  )
{% endif %}
GROUP BY
  tx_id
),
txn AS (
  SELECT
    *
  FROM
    {{ ref('silver_polygon__transactions') }}

{% if is_incremental() -%}
WHERE
  block_timestamp :: DATE >= (
    SELECT
      MAX(
        block_timestamp :: DATE
      )
    FROM
      {{ this }}
  )
{% endif %}
),
poly_labels AS (
  SELECT
    l1_label,
    l2_label,
    project_name,
    address_name,
    address
  FROM
    {{ ref('silver_crosschain__address_labels') }}
  WHERE
    blockchain = 'polygon'
),
poly_prices AS (
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
      'prices_v2'
    ) }}
    p
  WHERE
    p.asset_id = '3890'

{% if is_incremental() -%}
AND p.recorded_at :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ this }}
)
{% endif %}
GROUP BY
  p.symbol,
  DATE_TRUNC(
    'hour',
    recorded_at
  )
),
symbol AS (
  SELECT
    symbol,
    LOWER(token_address) AS token_address
  FROM
    {{ source(
      'shared',
      'market_asset_metadata'
    ) }}
  WHERE
    LOWER(token_address) = '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0'
  LIMIT
    1
)
SELECT
  t.block_timestamp,
  t.block_id AS block_id,
  t.tx_id,
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
  fee,
  fee * p.price AS fee_usd,
  CASE
    WHEN success = 1 THEN TRUE
    ELSE FALSE
  END AS success,
  COALESCE(
    event_count,
    0
  ) AS event_count
FROM
  txn t
  LEFT JOIN events e
  ON e.tx_id = t.tx_id
  LEFT JOIN {{ source(
    'ethereum',
    'sha256_function_signatures'
  ) }}
  f
  ON t.input_method = f.hex_signature
  AND f.importance = 1
  LEFT JOIN symbol C
  ON LOWER(
    t.to_address
  ) = C.token_address
  LEFT JOIN poly_prices p
  ON DATE_TRUNC(
    'hour',
    t.block_timestamp
  ) = p.hour
  LEFT JOIN poly_labels AS from_labels
  ON from_address = from_labels.address
  LEFT JOIN poly_labels AS to_labels
  ON to_address = to_labels.address
