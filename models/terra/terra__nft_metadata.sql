{{ config(
    materialized = 'view',
    tags = ['snowflake', 'terra_views', 'nft_metadata', 'terra']
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
    {{ ref('silver_terra__nft_metadata_galactic_punks') }}
