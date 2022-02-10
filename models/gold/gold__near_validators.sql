{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, block_number, address, status)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'gold', 'near', 'gold__near_validators', 'address_labels']
) }}

WITH near_labels AS (

  SELECT
    l1_label,
    l2_label,
    project_name,
    address_name,
    address
  FROM
    {{ source(
      'shared',
      'udm_address_labels'
    ) }}
  WHERE
    blockchain = 'near'
),
near_prices AS (
  SELECT
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
  WHERE
    symbol = 'NEAR'
  GROUP BY
    HOUR
)
SELECT
  'near' AS blockchain,
  v.block_timestamp,
  v.block_id AS block_number,
  v.account_id AS address,
  address_labels.l1_label AS address_label_type,
  address_labels.l2_label AS address_label_subtype,
  address_labels.project_name AS address_label,
  address_labels.address_name AS address_name,
  v.stake / 10e24 AS stake_amount,
  (
    v.stake / 10e24
  ) * stake_price.price AS stake_amount_usd,
  v.is_slashed,
  v.status,
  v.expected_blocks,
  v.produced_blocks,
  v.epoch_start_block
FROM
  {{ source(
    'near',
    'near_validators'
  ) }}
  v
  LEFT OUTER JOIN near_labels AS address_labels
  ON v.account_id = address_labels.address
  LEFT OUTER JOIN near_prices AS stake_price
  ON stake_price.hour = DATE_TRUNC(
    'hour',
    v.block_timestamp
  )
WHERE

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '3 days'
{% else %}
  block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
