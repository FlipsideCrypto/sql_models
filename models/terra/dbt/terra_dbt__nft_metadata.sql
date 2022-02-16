{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', contract_address, token_id)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'terra_silver', 'terra_dbt__nft_metadata']
) }}

WITH galactic_punks AS (
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
                '"testing-1635197180"'
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
        'terra' AS blockchain,
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
            CONCAT(
                '{"traits": ',
                PARSE_JSON(
                    SPLIT_PART(SPLIT_PART(VALUE :attributes :: STRING, '[', 2), ']', 1)
                ),
                ', "additional_metadata": ',
                PARSE_JSON(
                    VALUE :additional_metadata
                ),
                '}'
            )
        ) AS token_metadata,
        NULL AS token_metadata_uri,
        VALUE :name AS token_name
    FROM
        base_tables,
        LATERAL FLATTEN (
            input => record_content
        ) t
), 

terra_nft_metadata AS (
     WITH base_tables AS (

        SELECT
            *
        FROM
            {{ source(
                'bronze',
                'prod_nft_metadata_uploads_1828572827'
            ) }}
        WHERE
             SPLIT(
            record_content :model :sinks [0] :destination :: STRING,
            '.'
            ) [2] :: STRING = 'nft_metadata'
            AND record_content :model :blockchain :: STRING = 'terra'
            )

    {% if is_incremental() %}
    AND (
        record_metadata :CreateTime :: INT / 1000
    ) :: TIMESTAMP :: DATE >= (
        SELECT
            DATEADD('day', -10, MAX(system_created_at :: DATE))
        FROM
            {{ this }}
    )
    {% endif %}

    SELECT
        (
            record_metadata :CreateTime :: INT / 1000
        ) :: TIMESTAMP AS system_created_at,
        'terra' AS blockchain,
       t.value :commission_rate :: FLOAT AS commission_rate,
       t.value :contract_address :: STRING AS contract_address,
       t.value :contract_name :: STRING AS contract_name,
       t.value :created_at_block_id :: bigint AS created_at_block_id,
       t.value :created_at_timestamp :: TIMESTAMP AS created_at_timestamp,
       t.value :created_at_tx_id :: STRING AS created_at_tx_id,
       t.value :creator_address :: STRING AS creator_address,
       t.value :creator_name :: STRING AS creator_name,
       t.value :image_url :: STRING AS image_url,
       t.value :project_name :: STRING AS project_name,
       t.value :token_id :: STRING AS token_id,
       t.value :token_metadata :: OBJECT AS token_metadata,
       t.value :token_metadata_uri :: STRING AS token_metadata_uri,
       t.value :token_name :: STRING AS token_name,*
    FROM
        base_tables,
        LATERAL FLATTEN (
            input => record_content:results
        ) t
)

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

FROM terra_nft_metadata 

UNION ALL 

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

FROM galactic_punks