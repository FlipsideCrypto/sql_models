{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'nft']
  )
}}

WITH token_transfers AS (
  SELECT 
    tx_id, contract_addr AS contract_address, 
    event_inputs:from AS seller, 
    event_inputs:to AS buyer, 
    event_inputs:tokenId AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'Transfer' 
    AND 
    tx_id IN (

        SELECT tx_hash 
        FROM {{ source('ethereum', 'ethereum_events') }} 
        WHERE input_method IN ('0x9cec6392', '0x9cec639')
          {% if is_incremental() %}
            AND block_timestamp >= getdate() - interval '5 days'
          {% else %}
            AND block_timestamp >= getdate() - interval '9 months'
          {% endif %}

    ) 
    AND token_id IS NOT NULL
    {% if is_incremental() %}
       AND block_timestamp >= getdate() - interval '5 days'
    {% else %}
       AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}

  UNION

  SELECT 
    tx_id, 
    contract_addr AS contract_address,
    event_inputs:_from AS seller, 
    event_inputs:_to AS buyer, 
    event_inputs:_id AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'TransferSingle' 
    AND
    tx_id IN (

        SELECT tx_hash 
        FROM {{ source('ethereum', 'ethereum_events') }} 
        WHERE input_method IN ('0x9cec6392', '0x9cec639')
          {% if is_incremental() %}
            AND block_timestamp >= getdate() - interval '5 days'
          {% else %}
            AND block_timestamp >= getdate() - interval '9 months'
          {% endif %}
          
      ) 
    AND token_id IS NOT NULL
    {% if is_incremental() %}
       AND block_timestamp >= getdate() - interval '5 days'
    {% else %}
       AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}

  UNION

  SELECT 
    tx_id, 
    contract_addr AS contract_address,
    event_inputs:_from AS seller, 
    event_inputs:_to AS buyer, 
    event_inputs:_tokenId AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'Transfer' 
    AND
    tx_id IN (  

        SELECT tx_hash 
        FROM {{ source('ethereum', 'ethereum_events') }} 
        WHERE input_method IN ('0x9cec6392', '0x9cec639')
          {% if is_incremental() %}
            AND block_timestamp >= getdate() - interval '5 days'
          {% else %}
            AND block_timestamp >= getdate() - interval '9 months'
          {% endif %}
          
     ) 
    AND token_id IS NOT NULL
    {% if is_incremental() %}
       AND block_timestamp >= getdate() - interval '5 days'
    {% else %}
       AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}

),
nfts_per_tx AS (
  SELECT tx_id, count(token_id) as n_tokens
  FROM token_transfers
  GROUP BY 
    tx_id
),
tx_buyer_seller AS (
  SELECT tx_id, buyer, seller
  FROM token_transfers
  WHERE 
    token_id IS NOT NULL
  GROUP BY 
    tx_id, 
    buyer, 
    seller
),
token_transfer_events AS (
  SELECT 
    ee.tx_id, 
    block_timestamp, 
    origin_address,
    from_address, 
    to_address, 
    symbol, 
    amount, 
    buyer, 
    seller
  FROM {{ ref('ethereum__udm_events') }} ee
  JOIN tx_buyer_seller tbs 
    ON ee.tx_id = tbs.tx_id
  WHERE 
    ee.tx_id IN (SELECT tx_id FROM token_transfers WHERE token_id IS NOT NULL)
    AND 
    amount > 0 
    AND 
    (symbol != 'ETH' OR (symbol = 'ETH' AND from_address = '0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06'))
),
fee_tracking AS (
  SELECT
    tx_id, 
    block_timestamp, 
    from_address, 
    to_address, 
    symbol, 
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
    block_timestamp, 
    from_address, 
    to_address, 
    symbol, 
    amount AS platform_fee, 
    buyer, 
    seller
FROM fee_tracking
WHERE rank_amount = 1
),

creator_fee AS (
SELECT 
    tx_id, 
    block_timestamp, 
    from_address, 
    to_address, 
    symbol, 
    amount AS creator_fee, 
    buyer, 
    seller
FROM fee_tracking
WHERE 
  rank_amount = 2 
  AND n_transfers = 3
),

sale_price AS (
SELECT 
    tx_id, 
    block_timestamp, 
    symbol, 
    sum(amount) AS price
FROM token_transfer_events
WHERE buyer = origin_address
GROUP BY 
  tx_id, 
  block_timestamp, 
  symbol
)

SELECT
  'rarible' AS event_platform,
  tt.tx_id, 
  sale_price.block_timestamp, 
  'sale' AS event_type,
  tt.contract_address,
  token_id,
  tt.seller AS event_from,
  tt.buyer AS event_to, 
  price / n_tokens AS price,
  platform_fee / n_tokens AS platform_fee, 
  creator_fee / n_tokens AS creator_fee,
  sale_price.symbol AS tx_currency
FROM token_transfers tt

JOIN sale_price 
  ON tt.tx_id = sale_price.tx_id

JOIN platform_fee 
  ON tt.tx_id = platform_fee.tx_id

JOIN creator_fee 
  ON tt.tx_id = creator_fee.tx_id
  
JOIN nfts_per_tx 
  ON tt.tx_id = nfts_per_tx.tx_id


