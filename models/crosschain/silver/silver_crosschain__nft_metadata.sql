{{ config(
    materialized = 'incremental',
    unique_key = 'blockchain || contract_address || token_id',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'terra_silver', 'ethereum_silver', 'nft', 'silver_crosschain__nft_metadata']
) }}

WITH silver AS (

    SELECT
        system_created_at,
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
        {{ ref('ethereum_dbt__nft_metadata') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND created_at_timestamp :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(created_at_timestamp :: DATE))
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    '2000-01-01' :: TIMESTAMP AS system_created_at,
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
    {{ source(
        'ethereum',
        'nft_metadata'
    ) }}
WHERE
    1 = 1

{% if is_incremental() %}
AND created_at_timestamp :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(created_at_timestamp :: DATE))
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
    -- THIS SECTION CURRENTLY PULLS GALACTIC PUNK METADATA ONLY
    -- UNION IN OTHER METADATA AS NEEDED
SELECT
    system_created_at,
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
    {{ ref('terra_dbt__nft_metadata_galactic_punks') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND created_at_timestamp :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(created_at_timestamp :: DATE))
    FROM
        {{ this }}
)
{% endif %}
)
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
    silver qualify(ROW_NUMBER() over(PARTITION BY blockchain, contract_address, token_id
ORDER BY
    created_at_timestamp DESC)) = 1
