{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'nft']
  )
}}

WITH 
nf_token_transfers AS (
  SELECT 
    tx_id, 
    block_timestamp, 
    event_inputs:to AS buyer, 
    event_inputs:tokenId AS token_id, 
    contract_addr AS contract_address
  FROM {{ source('ethereum', 'ethereum_events_emitted') }}
  WHERE 
    tx_id IN (
        SELECT tx_id 
        FROM {{ ref('ethereum__udm_events') }}  
        WHERE origin_function_signature = '0x454a2ab3'
    )
    AND event_name = 'Transfer'
    AND token_id IS NOT NULL
    AND
      {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
      {% else %}
      block_timestamp >= getdate() - interval '9 months'
      {% endif %}
),

token_transfers AS (
  SELECT 
    block_timestamp,
    block_id,
    tx_id,
    origin_address,
    origin_label_type,
    origin_label_subtype,
    origin_label,
    origin_address_name,
    origin_function_signature,
    origin_function_name,
    from_address,
    from_label_type,
    from_label_subtype,
    from_label,
    from_address_name,
    to_address,
    to_label_type,
    to_label_subtype,
    to_label,
    to_address_name,
    event_name,
    event_type,
    event_id,
    contract_address,
    symbol,
    amount,
    amount_usd
  FROM {{ ref('ethereum__udm_events') }}
  WHERE 
    amount > 0 
    AND origin_function_signature = '0x454a2ab3'
    AND symbol = 'ETH'
    AND
    {% if is_incremental() %}
     block_timestamp >= getdate() - interval '1 days'
    {% else %}
     block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),

sent_amounts AS (
  SELECT 
    tx_id, 
    from_address AS address,
    sum(amount) AS sent_amount
  FROM token_transfers
  WHERE 
    amount > 0 
    AND 
    (from_address = '0xb1690c08e213a35ed9bab7b318de14420fb57d8c' OR origin_address = from_address)
    AND origin_function_signature = '0x454a2ab3'
  GROUP BY 
    tx_id, 
    address
),

rec_amounts AS (
  SELECT 
    tx_id, 
    to_address AS address,
    sum(amount) AS rec_amount
  FROM token_transfers
  WHERE 
    amount > 0 
    AND 
    (to_address = '0xb1690c08e213a35ed9bab7b318de14420fb57d8c' OR origin_address = to_address)
    AND origin_function_signature = '0x454a2ab3'
  GROUP BY 
    tx_id, 
    address
),

ck_fee AS (
  SELECT
    sent.tx_id,
    coalesce(rec_amount, 0) - coalesce(sent_amount, 0) AS platform_fee
  FROM sent_amounts sent
  JOIN rec_amounts rec 
    ON sent.address = rec.address
    AND sent.tx_id = rec.tx_id
  WHERE rec.address = '0xb1690c08e213a35ed9bab7b318de14420fb57d8c'
),
  
sale_amount AS (
  SELECT
    sent.tx_id, 
    coalesce(sent_amount, 0) - coalesce(rec_amount, 0) AS price
  FROM sent_amounts sent
  JOIN rec_amounts rec 
    ON sent.address = rec.address
    AND sent.tx_id = rec.tx_id
  WHERE rec.address != '0xb1690c08e213a35ed9bab7b318de14420fb57d8c'
),
seller AS (
  SELECT
    tx_id, 
    to_address AS seller
  FROM token_transfers tt
  WHERE 
    amount > 0
    AND origin_function_signature = '0x454a2ab3'
    AND from_address = '0xb1690c08e213a35ed9bab7b318de14420fb57d8c'
    AND to_address != origin_address
)
  
SELECT
  'crypto_kitties' AS event_platform,
  ntt.tx_id, 
  block_timestamp, 
  'sale' AS event_type,
  ntt.contract_address,
  token_id,
  seller AS event_from,
  buyer AS event_to, 
  price,
  platform_fee, 
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM nf_token_transfers ntt
JOIN seller 
  ON ntt.tx_id = seller.tx_id
JOIN ck_fee 
  ON ntt.tx_id = ck_fee.tx_id
JOIN sale_amount 
  ON ntt.tx_id = sale_amount.tx_id
