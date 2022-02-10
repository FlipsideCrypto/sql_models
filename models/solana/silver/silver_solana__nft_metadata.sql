{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', contract_name, token_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['created_at_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_nfts', 'solana_nft_metadata']
) }}

SELECT
  c.blockchain,
  c.contract_address,
  c.contract_name,
  c.created_at_timestamp,
  m.mint, 
  c.creator_address,
  c.creator_name,
  c.image_url,
  c.project_name,
  c.token_id,
  c.token_metadata,
  c.token_metadata_uri,
  c.token_name
FROM
  {{ ref('silver_crosschain__nft_metadata') }} c

LEFT OUTER JOIN {{ ref('solana_dbt__nft_metadata') }} m
ON c.token_id = m.token_id AND c.contract_address = m.contract_address

WHERE c.blockchain = 'solana'

{% if is_incremental() %}
    AND c.created_at_timestamp >= getdate() - interval '2 days'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY c.contract_name, c.token_id
ORDER BY
  c.created_at_timestamp DESC)) = 1