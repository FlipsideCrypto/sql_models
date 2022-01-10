{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events', 'solana_swaps']
) }}

WITH base_table AS (
    SELECT 
        block_timestamp, 
        block_id, 
        blockchain, 
        recent_blockhash, 
        tx_id,
        event_info[0]:parsed:info:source :: STRING AS SENDER, 
        COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING
        ) AS TOKEN_SENT, 
        CASE WHEN COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING 
        ) = '48F1neXh5bGgKr8G6CM6tFZkaC51UgtVb5pqGLC27Doi'
        OR COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING
        )= '2Zkvs84qBpMvWFSh9MFek2ZPAqAEjfufrs7Sbu42PbzBRTSv5Qx1bTWTGsbaSsuAPe4yx6ku3KmwVaGRbdANytF6'
        OR COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING 
        ) = 'DD6oh3HRCvMzqHkGeUW3za4pLgWNPJdV6aNYW3gVjXXi'
        OR COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING
        ) = 'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz'
        OR COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING 
        ) = '9wArqjNDHm43RJGSBSXuHygqJzEaW1xYA3XmmpU671kC'
        OR COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING
        ) = '5sHXydo4z6p2x7Jqfx8T4FnPRM9S2y8wpgNG1pAZf9Cg'
        OR COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING 
        ) = 'ArwS8uBqFTKNgLAy7r3poSCujCazc4GiqBZguUzQPrKj'
        OR COALESCE( 
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING 
        ) = '4aiKnDHFmNnLsopVsDyRBh8sbVohZYgdGzh3P9orpqNB'
        THEN event_meta[0]:parsed:info:amount/POW(10,9) :: INTEGER 
        ELSE event_meta[0]:parsed:info:amount/POW(10,6) :: INTEGER  END AS AMOUNT_SENT, 
        event_info[1]:parsed:info:mint :: STRING AS TOKEN_RECEIVED, 
        CASE WHEN event_info[1]:parsed:info:mint :: STRING = 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So' OR event_info[1]:parsed:info:mint :: STRING = 'StepAscQoEioFxxWGnh2sLBDFp9d8rvKz2Yp39iDpyT' OR event_info[1]:parsed:info:mint :: STRING = 'So11111111111111111111111111111111111111112' THEN COALESCE(
        event_meta[2]:parsed:info:amount/POW(10,9) :: INTEGER,
        event_meta[1]:parsed:info:amount/POW(10,9) :: INTEGER 
        ) 
        ELSE COALESCE(
        event_meta[2]:parsed:info:amount/POW(10,6) :: INTEGER,
        event_meta[1]:parsed:info:amount/POW(10,6) :: INTEGER 
        ) END AS AMOUNT_RECEIVED, 
        succeeded, 
        ingested_at
        
    FROM  {{ ref('silver_solana__events') }}

    WHERE event_info[1]:program = 'spl-token' 
    AND event_info[1]:programId = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND event_meta[0]:parsed:info:amount/POW(10,6) :: INTEGER IS NOT NULL -- IF THESE ARE NULL, THEN NFT MINT
    AND event_meta[1]:parsed:info:amount/POW(10,6) :: INTEGER IS NOT NULL

    {% if is_incremental() %}
  AND ingested_at >= (
    SELECT
      MAX(
        ingested_at
      )
    FROM
      {{ this }}
  )
  {% endif %}
), 

token_symbols AS (
    SELECT 
    m.asset_id, 
    p.name, 
    p.symbol, 
    m.token_address
FROM {{ source('shared', 'prices_v2')}} p 
JOIN {{ source('shared', 'market_asset_metadata') }} m ON m.asset_id = p.asset_id
WHERE m.token_address IS NOT NULL
{% if is_incremental() %}
  AND ingested_at >= (
    SELECT
      MAX(
        ingested_at
      )
    FROM
      {{ this }}
  )
  {% endif %}

ORDER BY p.recorded_at DESC
)

SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_blockhash, 
    tx_id,
    sender,
    token_sent,
    t.symbol AS token_sent_symbol,     
    amount_sent, 
    token_received, 
    f.symbol AS token_received_symbol, 
    amount_received, 
    succeeded, 
    ingested_at
FROM base_table b

LEFT OUTER JOIN token_symbols t
ON token_received = t.token_address

LEFT OUTER JOIN token_symbols f
ON token_received = f.token_address

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1