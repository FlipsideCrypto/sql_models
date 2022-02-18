{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_nfts', 'solana_nft_metadata']
) }}

SELECT
  blockchain,
  contract_address,
  contract_name,
  created_at_timestamp,
  mint,
  creator_address,
  creator_name,
  image_url,
  project_name,
  token_id,
  token_metadata,
  token_metadata_uri,
  token_name
FROM
  {{ ref('silver_solana__nft_metadata') }}