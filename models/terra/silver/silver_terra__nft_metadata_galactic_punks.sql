{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address || token_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id', 'tx_id'],
    tags = ['snowflake', 'terra_silver', 'terra_nft_metadata']
) }}

SELECT
    *
FROM
    {{ ref('terra_dbt__nft_metadata_galactic_punks') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(system_created_at :: DATE))
    FROM
        {{ this }} AS msgs
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY contract_address, token_id
ORDER BY
    system_created_at DESC)) = 1
