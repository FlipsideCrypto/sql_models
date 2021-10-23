{{ config(
  materialized = 'incremental',
  unique_key = 'contract_address || token_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft', 'ethereum__nft_metadata']
) }}

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
FROM
  {{ ref('silver_crosschain__nft_metadata') }}
WHERE
  blockchain = 'ethereum'
