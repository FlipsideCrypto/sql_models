{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

WITH makersplace_metadata AS (

  SELECT
    token_id,
    creator_address
  FROM
    {{ ref('ethereum__nft_metadata') }}
  WHERE
    contract_address = '0x2a46f2ffd99e19a89476e2f62270e0a35bbf0756'
),
eth_transfers AS (
  SELECT
    tx_id,
    block_timestamp,
    origin_address,
    from_address,
    to_address,
    amount
  FROM
    {{ ref('ethereum__udm_events') }}
  WHERE
    tx_id IN (
      SELECT
        tx_hash
      FROM
        {{ ref('silver_ethereum__events') }}
      WHERE
        input_method = '0xae77c237'
        AND contract_address = '0x2a46f2ffd99e19a89476e2f62270e0a35bbf0756'
    )
    AND symbol = 'ETH'
    AND amount > 0
    AND origin_address = from_address

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
),
nft_transfers AS (
  SELECT
    tx_id,
    block_timestamp,
    contract_address,
    creator_address,
    event_inputs :_from AS seller,
    event_inputs :_to AS buyer,
    event_inputs :_tokenId AS token_id
  FROM
    {{ ref('ethereum__events_emitted') }}
    ee
    LEFT OUTER JOIN makersplace_metadata meta
    ON ee.event_inputs :_tokenId = meta.token_id
  WHERE
    tx_id IN (
      SELECT
        tx_id
      FROM
        eth_transfers
    )

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
)
SELECT
  'makersplace' AS event_platform,
  et.tx_id,
  et.block_timestamp,
  'sale' AS event_type,
  nft.contract_address,
  token_id,
  seller AS event_from,
  buyer AS event_to,
  amount AS price,
  amount * 0.15 AS platform_fee,
  CASE
    WHEN seller != creator_address THEN amount * 0.10
    WHEN (
      seller = creator_address
      OR creator_address IS NULL
    ) THEN amount * 0.85
  END AS creator_fee,
  'ETH' AS tx_currency
FROM
  eth_transfers et
  JOIN nft_transfers nft
  ON et.tx_id = nft.tx_id
WHERE
  from_address = buyer
