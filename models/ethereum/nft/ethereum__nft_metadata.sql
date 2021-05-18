{{ 
  config(
    materialized='incremental', 
    sort='created_at_timestamp', 
    unique_key='token_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'nft']
  )
}}

SELECT
  blockchain,	
  commission_rate,
  contract_address,
  contract_name,
  created_at_block_id,
  created_at_timestamp,
  created_at_tx_id,
  creator_address,
  creator_name,
  image_url,
  project_name,
  token_id,
  token_metadata,
  token_metadata_uri,
  token_name
FROM {{ source('ethereum', 'nft_metadata') }}
WHERE blockchain = 'ethereum'

AND 
{% if is_incremental() %}
  created_at_timestamp >= getdate() - interval '1 days'
{% else %}
  created_at_timestamp >= getdate() - interval '9 months'
{% endif %}
