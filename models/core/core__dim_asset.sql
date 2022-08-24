{{ config(
    materialized = 'incremental',
    unique_key = 'dim_asset_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

WITH silver AS (

    SELECT
        asset_id,
        asset_name,
        total_supply,
        asset_url,
        decimals,
        asset_deleted,
        creator_address,
        created_at,
        closed_at,
        collection_name,
        collection_nft,
        arc69_nft,
        ar3_nft,
        ar19_nft,
        traditional_nft,
        is_nft,
        _inserted_timestamp
    FROM
        {{ ref('silver__asset') }}

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
    OR asset_id IN (
        SELECT
            asset_id
        FROM
            {{ this }}
        WHERE
            dim_account_id__creator = '-1'
            OR dim_block_id__created_at = '-1'
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['asset_id']
    ) }} AS dim_asset_id,
    asset_id,
    asset_name,
    total_supply,
    asset_url,
    decimals,
    asset_deleted,
    COALESCE(
        da.dim_account_id,
        '-1'
    ) AS dim_account_id__creator,
    A.creator_address AS creator_address,
    COALESCE(
        C.dim_block_id,
        '-1'
    ) AS dim_block_id__created_at,
    C.block_timestamp AS created_at,
    COALESCE(
        b.dim_block_id,
        '-2'
    ) AS dim_block_id__closed_at,
    b.block_timestamp AS closed_at,
    collection_name,
    collection_nft,
    arc69_nft,
    ar3_nft,
    ar19_nft,
    traditional_nft,
    is_nft,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    silver A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.closed_at = b.block_id
    JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
    JOIN {{ ref('core__dim_account') }}
    da
    ON A.creator_address = da.address
UNION ALL
SELECT
    '-1' AS dim_asset_id,
    -1 AS asset_id,
    'unknown' AS asset_name,
    NULL AS total_supply,
    NULL AS asset_url,
    NULL AS decimals,
    NULL AS asset_deleted,
    '-1' AS dim_account_id__creator,
    NULL AS creator_address,
    '-1' AS dim_block_id__created_at,
    NULL AS created_at,
    '-1' AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS collection_name,
    NULL AS collection_nft,
    NULL AS arc69_nft,
    NULL AS ar3_nft,
    NULL AS ar19_nft,
    NULL AS traditional_nft,
    NULL AS is_nft,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    '-2' AS dim_asset_id,
    -2 AS asset_id,
    'not applicable' AS asset_name,
    NULL AS total_supply,
    NULL AS asset_url,
    NULL AS decimals,
    NULL AS asset_deleted,
    '-2' AS dim_account_id__creator,
    NULL AS creator_address,
    '-2' AS dim_block_id__created_at,
    NULL AS created_at,
    '-2' AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS collection_name,
    NULL AS collection_nft,
    NULL AS arc69_nft,
    NULL AS ar3_nft,
    NULL AS ar19_nft,
    NULL AS traditional_nft,
    NULL AS is_nft,
    '1900-01-01' :: DATE _inserted_timestamp,
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
    '-2' AS dim_account_id__creator,
    NULL AS creator_address,
    '-2' AS dim_block_id__created_at,
    NULL AS created_at,
    '-2' AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS collection_name,
    NULL AS collection_nft,
    NULL AS arc69_nft,
    NULL AS ar3_nft,
    NULL AS ar19_nft,
    NULL AS traditional_nft,
    NULL AS is_nft,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.asset_id']
    ) }} AS dim_asset_id,
    A.asset_id AS asset_id,
    NULL AS asset_name,
    NULL AS total_supply,
    NULL AS asset_url,
    NULL AS decimals,
    FALSE AS asset_deleted,
    '-2' AS dim_account_id__creator,
    NULL AS creator_address,
    '-2' AS dim_block_id__created_at,
    NULL AS created_at,
    '-2' AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS collection_name,
    NULL AS collection_nft,
    NULL AS arc69_nft,
    NULL AS ar3_nft,
    NULL AS ar19_nft,
    NULL AS traditional_nft,
    NULL AS is_nft,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    (
        SELECT
            DISTINCT asset_id
        FROM
            {{ ref('silver__transaction') }}
        WHERE
            COALESCE(asset_id, 0) <> 0

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
) A
LEFT JOIN {{ ref('silver__asset') }}
b
ON A.asset_id = b.asset_id
WHERE
    b.asset_id IS NULL
