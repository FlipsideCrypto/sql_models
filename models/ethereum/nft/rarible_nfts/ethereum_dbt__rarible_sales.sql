{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'nft']
  )
}}

WITH rarible_txns AS (
  SELECT
    tx_hash AS tx_id
  FROM
  {{ source('ethereum', 'ethereum_transactions') }} WHERE to_address IN ('0x9757f2d2b135150bbeb65308d4a91804107cd8d6',
                              '0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06')
  {% if is_incremental() %}
    AND
    block_timestamp >= getdate() - interval '1 days'
  {% endif %}
  GROUP BY tx_id
),
token_transfers AS (
  SELECT 
    tx_id, 
    block_timestamp,
    contract_addr AS contract_address, 
    event_inputs:from AS seller, 
    event_inputs:to AS buyer, 
    event_inputs:tokenId AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'Transfer' AND
    tx_id IN (SELECT tx_id FROM rarible_txns) AND
    token_id IS NOT NULL
  {% if is_incremental() %}
    AND
    block_timestamp >= getdate() - interval '1 days'
  {% endif %}
  UNION
  SELECT 
    tx_id, 
    block_timestamp,
    contract_addr AS contract_address,
    event_inputs:_from AS seller, 
    event_inputs:_to AS buyer, 
    event_inputs:_id AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'TransferSingle' AND
    tx_id IN (SELECT tx_id FROM rarible_txns) AND
    token_id IS NOT NULL
  {% if is_incremental() %}
    AND
    block_timestamp >= getdate() - interval '1 days'
  {% endif %}
  UNION
  SELECT 
    tx_id, 
    block_timestamp,
    contract_addr AS contract_address,
    event_inputs:_from AS seller, 
    event_inputs:_to AS buyer, 
    event_inputs:_tokenId AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'Transfer' AND
    tx_id IN (SELECT tx_id FROM rarible_txns) AND
    token_id IS NOT NULL
  {% if is_incremental() %}
    AND
    block_timestamp >= getdate() - interval '1 days'
  {% endif %}
),
nfts_per_tx AS (
  SELECT tx_id, count(token_id) as n_tokens
  FROM token_transfers
  GROUP BY tx_id
),
tx_buyer_seller AS (
  SELECT tx_id, buyer, seller
  FROM token_transfers
  WHERE token_id IS NOT NULL
  GROUP BY tx_id, buyer, seller
),
token_transfer_events AS (
  SELECT 
    ee.tx_id, 
    origin_address,
    from_address, 
    to_address, 
    symbol,
    contract_address,
    amount, 
    buyer, 
    seller
  FROM {{ ref('ethereum__udm_events') }} ee
  JOIN tx_buyer_seller tbs ON ee.tx_id = tbs.tx_id
  WHERE ee.tx_id IN (SELECT tx_id FROM token_transfers WHERE token_id IS NOT NULL)
  AND 
  amount > 0 
  AND 
  (symbol != 'ETH' OR (symbol = 'ETH' AND from_address IN ('0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06', '0x9757f2d2b135150bbeb65308d4a91804107cd8d6')))
  {% if is_incremental() %}
    AND
    block_timestamp >= getdate() - interval '1 days'
  {% endif %}
),
fee_tracking AS (
  SELECT
    tx_id, 
    from_address, 
    to_address, 
    symbol, 
    contract_address,
    amount, 
    buyer, 
    seller, 
    RANK() OVER (PARTITION BY tx_id ORDER BY amount ASC) AS rank_amount,
    count(*) OVER (PARTITION BY tx_id) AS n_transfers
  FROM token_transfer_events
),
platform_fee AS (
SELECT 
    tx_id, 
    from_address, 
    to_address, 
    symbol, 
    contract_address,
    amount AS platform_fee, 
    buyer, 
    seller
FROM fee_tracking
WHERE rank_amount = 1
),

creator_fee AS (
SELECT 
    tx_id, 
    from_address, 
    to_address, 
    symbol, 
    contract_address,
    amount AS creator_fee, 
    buyer, 
    seller
FROM fee_tracking
WHERE rank_amount = 2 AND n_transfers = 3
),

sale_price AS (
SELECT 
    tx_id, 
    symbol, 
    contract_address,
    sum(amount) AS price
FROM token_transfer_events
WHERE buyer = origin_address
GROUP BY 
  tx_id, 
    symbol,
    contract_address
)

SELECT
  'rarible' AS event_platform,
  tt.tx_id, 
  tt.block_timestamp, 
  'sale' AS event_type,
  tt.contract_address,
  token_id,
  tt.seller AS event_from,
  tt.buyer AS event_to, 
  COALESCE(price / n_tokens, 0) AS price,
  COALESCE(platform_fee / n_tokens, 0) AS platform_fee, 
  COALESCE(creator_fee / n_tokens, 0) AS creator_fee,
  CASE WHEN sale_price.symbol IS NULL THEN sale_price.contract_address ELSE sale_price.symbol END AS tx_currency
FROM token_transfers tt
JOIN sale_price ON tt.tx_id = sale_price.tx_id
LEFT OUTER JOIN platform_fee ON tt.tx_id = platform_fee.tx_id
LEFT OUTER JOIN creator_fee ON tt.tx_id = creator_fee.tx_id
LEFT OUTER JOIN nfts_per_tx ON tt.tx_id = nfts_per_tx.tx_id