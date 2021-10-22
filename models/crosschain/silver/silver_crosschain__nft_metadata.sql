{{ config(
    materialized = 'view',
    unique_key = 'contract_address || token_id',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'terra_silver', 'silver_terra__nft_metadata']
) }}

WITH silver AS (
    -- THIS SECTION CURRENTLY PULLS GALACTIC PUNK METADATA ONLY
    -- UNION IN OTHER METADATA AS NEEDED

    SELECT
        *
    FROM
        {{ ref('terra_dbt__nft_metadata_galactic_punks') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
    SELECT
        DATEADD('day', -10, MAX(system_created_at :: DATE))
    FROM
        {{ this }} AS msgs
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY contract_address, token_id
ORDER BY
    created_at_timestamp DESC)) = 1
UNION ALL
SELECT
    *
FROM
    {{ ref('ethereum_dbt__nft_metadata') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
    SELECT
        DATEADD('day', -10, MAX(system_created_at :: DATE))
    FROM
        {{ this }} AS msgs
)
{% endif %}
)
