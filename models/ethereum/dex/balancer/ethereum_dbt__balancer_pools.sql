{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key= 'tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'dex']
  )
}}


-- all balancer v1 events
WITH balancer AS (

  SELECT DISTINCT contract_address AS balancer_pool, REGEXP_REPLACE(event_inputs:tokenIn,'\"','') AS token_contract_address  
  FROM {{ref('ethereum__events_emitted')}}
  WHERE event_name = 'LOG_SWAP'
  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
  AND event_inputs:caller IS NOT NULL

  UNION 

  SELECT DISTINCT contract_address AS balancer_pool, event_inputs:tokenOut AS token_contract_address  
  FROM {{ref('ethereum__events_emitted')}}
  WHERE event_name = 'LOG_SWAP'
  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
  AND event_inputs:caller IS NOT NULL
  
  UNION
   
  SELECT DISTINCT contract_address AS balancer_pool, REGEXP_REPLACE(event_inputs:tokenIn,'\"','') AS token_contract_address  
  FROM {{ref('ethereum__events_emitted')}}
  WHERE event_name = 'LOG_JOIN'
  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
  AND event_inputs:caller IS NOT NULL

  UNION 

  SELECT DISTINCT contract_address AS balancer_pool, event_inputs:tokenOut AS token_contract_address  
  FROM {{ref('ethereum__events_emitted')}}
  WHERE event_name = 'LOG_JOIN'
  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
  AND event_inputs:caller IS NOT NULL

), balancer_tokens_distinct AS (
    SELECT DISTINCT balancer_pool,token_contract_address FROM balancer
)


SELECT balancer_pool,ARRAY_AGG(token_contract_address) AS tokens FROM balancer_tokens_distinct
GROUP BY 1