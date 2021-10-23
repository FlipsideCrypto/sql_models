{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

WITH mints AS (

  SELECT
    tx_id,
    block_timestamp,
    token_id,
    event_from,
    event_to,
    contract_address
  FROM
    {{ ref('ethereum_dbt__hashmasks_mints') }}
  WHERE

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
n_tokens_per AS (
  SELECT
    tx_id,
    COUNT(token_id) AS n_tokens
  FROM
    mints
  GROUP BY
    tx_id
),
eth_transfers AS (
  SELECT
    "from",
    tx_hash AS tx_id,
    eth_value
  FROM
    {{ ref('silver_ethereum__events') }}
  WHERE
    tx_hash IN (
      SELECT
        tx_id
      FROM
        mints
    )
    AND eth_value > 0
    AND

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
)
SELECT
  'hashmasks' AS event_platform,
  mints.tx_id,
  mints.block_timestamp,
  'sale' AS event_type,
  contract_address,
  token_id,
  '0xc2c747e0f7004f9e8817db2ca4997657a7746928' AS event_from,
  event_to,
  eth_value / n_tokens AS price,
  eth_value / n_tokens AS platform_fee,
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM
  mints
  JOIN n_tokens_per
  ON mints.tx_id = n_tokens_per.tx_id
  JOIN eth_transfers et
  ON mints.tx_id = et.tx_id
WHERE
  "from" = event_to
