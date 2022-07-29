{{ config(
    materialized = 'incremental',
    unique_key = 'dim_asset_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

WITH prebase AS (

    SELECT
        A.index AS asset_id,
        algorand_decode_hex_addr(
            creator_addr :: text
        ) AS creator_address,
        A.params :au :: STRING AS asset_url,
        A.params,
        A.deleted,
        closed_at,
        created_at,
        A._inserted_timestamp
    FROM
        {{ ref('bronze__asset') }} A

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
base AS (
    SELECT
        A.asset_id,
        creator_address,
        asset_url,
        CASE
            WHEN A.deleted = 'TRUE'
            AND ac.asset_id IS NOT NULL THEN ac.asset_name
            ELSE A.params :an :: STRING
        END asset_name,
        CASE
            WHEN A.deleted = 'TRUE'
            AND ac.asset_id IS NOT NULL THEN ac.asset_amount
            ELSE A.params :t :: NUMBER
        END AS total_supply,
        CASE
            WHEN A.deleted = 'TRUE'
            AND ac.asset_id IS NOT NULL THEN ac.decimals
            WHEN A.params :dc IS NULL THEN 0
            WHEN A.params :dc IS NOT NULL THEN params :dc :: NUMBER
        END AS decimals,
        A.deleted,
        closed_at,
        created_at,
        A._inserted_timestamp
    FROM
        prebase A
        LEFT JOIN {{ ref('silver__asset_config') }}
        ac
        ON A.asset_id = ac.asset_id
),
collect_NFTs AS(
    SELECT
        *
    FROM
        {{ source(
            'bronze',
            'prod_nft_metadata_uploads_1828572827'
        ) }}
    WHERE
        record_metadata :key LIKE '%algo-nft-meta%'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
collection_NFTs AS (
    SELECT
        VALUE :asset :: NUMBER AS nft,
        VALUE :collection :: STRING AS collection,
        VALUE :manager :: STRING AS manager,
        VALUE :name :: STRING AS NAME,
        VALUE :url :: STRING AS url,
        _inserted_timestamp
    FROM
        collect_NFTs,
        LATERAL FLATTEN(
            input => record_content
        ) f
    WHERE
        VALUE :asset :: STRING <> '' qualify(ROW_NUMBER() over(PARTITION BY nft
    ORDER BY
        INDEX)) = 1
),
arc69_NFTs AS(
    SELECT
        asset_id AS nft,
        MAX(
            A._inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }} A
    WHERE
        tx_type = 'acfg'
        AND TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                tx_message :txn :note :: STRING
            )
        ) :standard :: STRING = 'arc69'

{% if is_incremental() %}
AND A._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    asset_id
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.asset_id']
    ) }} AS dim_asset_id,
    A.asset_id,
    COALESCE(
        A.asset_name,
        coll.name
    ) AS asset_name,
    A.total_supply,
    COALESCE(
        asset_url,
        coll.url
    ) AS asset_url,
    A.decimals,
    deleted AS asset_deleted,
    COALESCE(
        da.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id__creator,
    da.address AS creator_address,
    COALESCE(
        C.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__created_at,
    C.block_timestamp AS created_at,
    COALESCE(
        b.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__closed_at,
    b.block_timestamp AS closed_at,
    coll.collection AS collection_name,
    CASE
        WHEN coll.nft IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS collection_nft,
    CASE
        WHEN arc69.nft IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS arc69_nft,
    CASE
        WHEN COALESCE(
            asset_url,
            coll.url
        ) LIKE '%#arc3%' THEN TRUE
        ELSE FALSE
    END AS ar3_nft,
    CASE
        WHEN A.decimals = 0
        AND A.total_supply = 1 THEN TRUE
        ELSE FALSE
    END AS traditional_nft,
    CASE
        WHEN coll.nft IS NOT NULL
        OR arc69.nft IS NOT NULL
        OR COALESCE(
            asset_url,
            coll.url
        ) LIKE '%#arc3%'
        OR (
            A.decimals = 0
            AND A.total_supply = 1
        ) THEN TRUE
        ELSE FALSE
    END AS is_nft,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.closed_at = b.block_id
    JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
    JOIN {{ ref('core__dim_account') }}
    da
    ON A.creator_address = da.address
    LEFT JOIN collection_NFTs coll
    ON A.asset_id = coll.nft
    LEFT JOIN arc69_NFTs arc69
    ON A.asset_id = arc69.nft
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_asset_id,
    NULL AS asset_id,
    NULL AS asset_name,
    NULL AS total_supply,
    NULL AS asset_url,
    NULL AS decimals,
    NULL AS asset_deleted,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_account_id__creator,
    NULL AS creator_address,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_block_id__created_at,
    NULL AS created_at,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS collection_name,
    NULL AS collection_nft,
    NULL AS arc69_nft,
    NULL AS ar3_nft,
    NULL AS traditional_nft,
    NULL AS is_nft,
    CURRENT_DATE AS _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['0']
    ) }} AS dim_asset_id,
    0 AS asset_id,
    'ALGO' AS asset_name,
    NULL AS total_supply,
    NULL AS asset_url,
    6 AS decimals,
    FALSE AS asset_deleted,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_account_id__creator,
    NULL AS creator_address,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_block_id__created_at,
    NULL AS created_at,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS collection_name,
    NULL AS collection_nft,
    NULL AS arc69_nft,
    NULL AS ar3_nft,
    NULL AS traditional_nft,
    NULL AS is_nft,
    CURRENT_DATE AS _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
