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
    tx_id, 
    contract_addr AS contract_address, 
    event_inputs:from AS seller, 
    event_inputs:to AS buyer, 
    event_inputs:tokenId AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'Transfer'
   
   AND tx_id IN (SELECT tx_hash 
                 FROM {{ source('ethereum', 'ethereum_events') }} 
                 WHERE input_method = '0xab834bab')
   
   AND token_id IS NOT NULL
  
  UNION
  
  SELECT 
    tx_id, 
    contract_addr AS contract_address, 
    event_inputs:_from AS seller, 
    event_inputs:_to AS buyer, 
    event_inputs:_id AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'TransferSingle'
   
   AND tx_id IN (SELECT tx_hash 
                 FROM {{ source('ethereum', 'ethereum_events') }} 
                 WHERE input_method = '0xab834bab')
   
   AND token_id IS NOT NULL
  
  UNION
  
  SELECT 
    tx_id, 
    contract_addr AS contract_address, 
    event_inputs:_from AS seller, 
    event_inputs:_to AS buyer, 
    event_inputs:_tokenId AS token_id
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE event_name = 'Transfer'
   
   AND tx_id IN (SELECT tx_hash 
                 FROM {{ source('ethereum', 'ethereum_events') }} 
                 WHERE input_method = '0xab834bab')
   
   AND token_id IS NOT NULL
),

nfts_per_tx AS (
  SELECT 
    tx_id, 
    count(token_id) as n_tokens
  FROM token_transfers
  GROUP BY tx_id
  
),

tx_buyer_seller AS (
  SELECT 
    tx_id, 
    buyer, 
    seller
  FROM token_transfers
  WHERE token_id IS NOT NULL
  GROUP BY tx_id, buyer, seller
),

token_transfer_events AS (
  SELECT 
    ee.tx_id, 
    block_timestamp, 
    from_address, 
    buyer, 
    seller, 
    to_address, 
    symbol, 
    amount
  FROM {{ ref('ethereum__udm_events') }} ee
  
  JOIN tx_buyer_seller tbs 
    ON ee.tx_id = tbs.tx_id

  WHERE ee.tx_id IN (SELECT tx_id 
                     FROM token_transfers 
                     WHERE token_id IS NOT NULL)
  
  AND amount > 0
  
  AND
    {% if is_incremental() %}
     block_timestamp >= getdate() - interval '1 days'
    {% else %}
     block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),

tx_paid AS (
  SELECT 
    tx_id, 
    amount AS price, 
    symbol AS tx_currency
  FROM token_transfer_events
  WHERE from_address = buyer
),

platform_fees AS (
  SELECT 
    tx_id, 
    block_timestamp, 
    amount AS platform_fee
  FROM token_transfer_events
  WHERE to_address = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
)

SELECT
    'opensea' AS event_platform,
    tt.tx_id, 
    block_timestamp, 
    'sale' AS event_type,
    tt.contract_address,
    token_id,
    seller AS event_from,
    buyer AS event_to, 
    price / n_tokens AS price,
    platform_fee / n_tokens AS platform_fee, 
    0 AS creator_fee,
    tx_currency
FROM token_transfers tt

JOIN tx_paid 
  ON tt.tx_id = tx_paid.tx_id

JOIN platform_fees 
  ON tt.tx_id = platform_fees.tx_id

JOIN nfts_per_tx 
  ON tt.tx_id = nfts_per_tx.tx_id

WHERE
{% if is_incremental() %}
    block_timestamp >= getdate() - interval '1 days'
{% else %}
    block_timestamp >= getdate() - interval '9 months'
{% endif %}
