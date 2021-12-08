{{
    config(
        materialized ='incremental', 
        unique_key="CONCAT_WS('-', BLOCK_ID)", 
        incremental_strategy='delete+insert', 
        tags=['snowflake', 'solana', 'transactions'] 
    )
}}

WITH base_tables AS (

    SELECT * 
    FROM 
        {{ source(
           'bronze_chainwalkers', 
           'solana_blocks'
        ) }}
    WHERE 
        1 = 1

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp
    )
  FROM
    {{ this }} 
)
{% endif %}
), 

basic_data AS (
    SELECT
        ingested_at AS system_created_at, 
        offset_id, 
        block_id, 
        block_timestamp, 
        network, 
        chain_id, 
        tx_count
    FROM 
        base_tables 
), 

transaction_data AS (
    SELECT
        t.value :meta:fee :: INTEGER AS fee, 
        t.value :meta:postBalances :: ARRAY AS post_balances, 
        t.value :meta:postTokenBalances :: ARRAY AS post_token_balances, 
        t.value :meta:preBalances :: ARRAY AS pre_balances, 
        t.value :meta:preTokenBalances :: ARRAY AS pre_token_balances, 
        t.value :meta:rewards :: ARRAY AS rewards, 
        t.value :meta:status:Err :: ARRAY AS error,
        t.value :transaction:message:accountKeys :: ARRAY AS transactions, 
        t.value :transaction:message:instructions :: ARRAY AS instructions, 
        t.value :transaction:message:recentBlockhash :: STRING AS recent_blockhash, 
        SUBSTR(t.value :transaction:signatures :: STRING, 3, LENGTH(t.value :transaction:signatures :: STRING) - 4) AS signature
    FROM
       base_tables, 
        LATERAL FLATTEN(
            input => txs
        ) t, 
        LATERAL FLATTEN(
            input => t.value :transaction:message:instructions
        ) instructions
)



