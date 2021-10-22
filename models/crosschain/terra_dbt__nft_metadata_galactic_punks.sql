{{ config(
    materialized = 'view',
    unique_key = 'contract_address || token_id',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'terra_silver', 'terra_dbt__nft_metadata']
) }}

WITH base_tables AS (

    SELECT
        *
    FROM
        {{ source(
            'bronze',
            'JIM_TEST_REST_PROXY_1507016047'
        ) }}
    WHERE
        record_metadata :key :: STRING IN (
            '"testing-1634323727"'
        )

{% if is_incremental() %}
--- does this need a look back? It's static data
AND (
    record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
    SELECT
        DATEADD('day', -10, MAX(system_created_at :: DATE))
    FROM
        {{ this }}
)
{% endif %}
) -----
SELECT
    (
        record_metadata :CreateTime :: INT / 1000
    ) :: TIMESTAMP AS system_created_at,
    'Terra' AS blockchain,
    NULL AS commission_rate,
    VALUE :collection_addr :: STRING AS contract_address,
    'Galactic Punks' AS contract_name,
    NULL AS created_at_block_id,
    VALUE :created_at :: TIMESTAMP AS created_at_timestamp,
    NULL AS created_at_tx_id,
    NULL AS creator_address,
    NULL AS creator_name,
    VALUE :src :: STRING AS image_url,
    'Galactic Punks' AS project_name,
    VALUE :token_id :: STRING AS token_id,
    PARSE_JSON(
        SPLIT_PART(SPLIT_PART(VALUE :attributes :: STRING, '[', 2), ']', 1)
    ) AS token_metadata,
    NULL AS token_metadata_uri,
    VALUE :name AS token_name
FROM
    base_tables,
    LATERAL FLATTEN (
        input => record_content
    ) t
