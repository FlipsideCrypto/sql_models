{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

WITH creators AS (

  SELECT
    token_id,
    event_to AS creator
  FROM
    {{ ref('ethereum_dbt__superrare_mints') }}
),
buy_txids AS (
  SELECT
    tx_hash AS tx_id,
    block_timestamp
  FROM
    {{ ref('silver_ethereum__events') }}
  WHERE
    input_method IN (
      '0xcce7ec13',
      '0xd96a094a'
    )
    AND

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
GROUP BY
  tx_id,
  block_timestamp
),
buy_nf_transfers AS (
  SELECT
    tx_id,
    block_timestamp,
    contract_addr AS contract_address,
    event_inputs :from AS seller,
    event_inputs :to AS buyer,
    event_inputs :tokenId AS token_id
  FROM
    {{ ref('silver_ethereum__events_emitted') }}
  WHERE
    tx_id IN (
      SELECT
        tx_id
      FROM
        buy_txids
    )
    AND contract_addr = '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'
    AND

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
buy_eth_transfers AS (
  SELECT
    ee.tx_id,
    ee.block_timestamp,
    ee.from_address,
    origin_address,
    to_address,
    amount,
    seller,
    buyer,
    nft.token_id
  FROM
    {{ ref('ethereum__udm_events') }}
    ee
    INNER JOIN buy_nf_transfers nft
    ON ee.tx_id = nft.tx_id
    INNER JOIN creators
    ON nft.token_id = creators.token_id
  WHERE
    ee.tx_id IN (
      SELECT
        tx_id
      FROM
        buy_txids
    )
    AND amount > 0
    AND symbol = 'ETH'
    AND

{% if is_incremental() %}
ee.block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  ee.block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
),
buy_platform_fee AS (
  SELECT
    tx_id,
    block_timestamp,
    SUM(amount) AS platform_fee
  FROM
    buy_eth_transfers
  WHERE
    to_address = '0xb2d39da392f19edb27639b92adfe7edfcc96391b'
  GROUP BY
    tx_id,
    block_timestamp
),
buy_creator_fee AS (
  SELECT
    ee.tx_id,
    ee.block_timestamp,
    SUM(amount) AS creator_fee
  FROM
    buy_eth_transfers ee
    INNER JOIN creators
    ON ee.token_id = creators.token_id
  WHERE
    to_address = creator
  GROUP BY
    ee.tx_id,
    ee.block_timestamp
),
buy_price AS (
  SELECT
    ee.tx_id,
    ee.block_timestamp,
    SUM(amount) AS price
  FROM
    buy_eth_transfers ee
  WHERE
    from_address = origin_address
  GROUP BY
    ee.tx_id,
    ee.block_timestamp
)
SELECT
  'superrare' AS event_platform,
  nft.tx_id,
  nft.block_timestamp,
  'sale' AS event_type,
  contract_address,
  token_id,
  seller AS event_from,
  buyer AS event_to,
  price,
  platform_fee,
  creator_fee,
  'ETH' AS tx_currency
FROM
  buy_nf_transfers nft
  INNER JOIN buy_platform_fee platform
  ON nft.tx_id = platform.tx_id
  INNER JOIN buy_creator_fee creator
  ON nft.tx_id = creator.tx_id
  INNER JOIN buy_price price
  ON nft.tx_id = price.tx_id
