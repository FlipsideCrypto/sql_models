{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', contract_name, token_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['created_at_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_nfts', 'solana_nft_metadata']
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
  {{ ref('solana_dbt__nft_metadata') }} 

WHERE blockchain = 'solana'

{% if is_incremental() %}
    AND created_at_timestamp >= getdate() - interval '2 days'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY contract_name, token_id
ORDER BY
  created_at_timestamp DESC)) = 1