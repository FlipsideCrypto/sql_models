{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

WITH mints AS (

  SELECT
    tx_id,
    token_id,
    event_to AS buyer,
    contract_address AS platform_contract
  FROM
    {{ ref('ethereum_dbt__art_blocks_mints') }}
),
token_transfers AS (
  SELECT
    ee.tx_hash AS tx_id,
    ee.block_timestamp,
    "from",
    "to",
    buyer,
    token_id,
    platform_contract,
    eth_value
  FROM
    {{ ref('silver_ethereum__events') }}
    ee
    INNER JOIN mints
    ON ee.tx_hash = mints.tx_id
  WHERE
    eth_value > 0
    AND contract_address IS NULL
    

{% if is_incremental() %}
AND ee.block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
max_xfer AS (
  SELECT
    tx_id,
    MAX(eth_value) AS max_value
  FROM
    token_transfers
  WHERE
    "from" != buyer
  GROUP BY
    tx_id
),
creator_fees AS (
  SELECT
    tt.tx_id,
    eth_value AS creator_fee,
    "to" AS creator
  FROM
    token_transfers tt
    JOIN max_xfer
    ON tt.tx_id = max_xfer.tx_id
  WHERE
    eth_value = max_value
),
platform_fees AS (
  SELECT
    tx_id,
    MIN(eth_value) AS platform_fee
  FROM
    token_transfers
  WHERE
    "from" != buyer
  GROUP BY
    tx_id
)
SELECT
  'art_blocks' AS event_platform,
  tt.tx_id,
  block_timestamp,
  'sale' AS event_type,
  platform_contract AS contract_address,
  token_id,
  creator AS event_from,
  buyer AS event_to,
  eth_value AS price,
  platform_fee,
  creator_fee,
  'ETH' AS tx_currency
FROM
  token_transfers tt
  JOIN creator_fees
  ON tt.tx_id = creator_fees.tx_id
  JOIN platform_fees
  ON tt.tx_id = platform_fees.tx_id
WHERE
  "from" = buyer
