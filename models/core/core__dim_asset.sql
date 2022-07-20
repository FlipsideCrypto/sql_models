{{ config(
    materialized = 'incremental',
    unique_key = 'dim_asset_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

WITH base AS (

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
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.asset_id']
    ) }} AS dim_asset_id,
    A.asset_id,
    CASE
        WHEN A.deleted = 'TRUE'
        AND ac.asset_id IS NOT NULL THEN ac.asset_name
        ELSE A.params :an :: STRING
    END AS asset_name,
    CASE
        WHEN A.deleted = 'TRUE'
        AND ac.asset_id IS NOT NULL THEN ac.asset_amount
        ELSE A.params :t :: NUMBER
    END AS total_supply,
    asset_url,
    CASE
        WHEN A.deleted = 'TRUE'
        AND ac.asset_id IS NOT NULL THEN ac.decimals
        WHEN A.params :dc IS NULL THEN 0
        WHEN A.params :dc IS NOT NULL THEN params :dc :: NUMBER
    END AS decimals,
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
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.closed_at = b.block_id
    LEFT JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
    LEFT JOIN {{ ref('silver__asset_config') }}
    ac
    ON A.asset_ID = ac.asset_ID
    LEFT JOIN {{ ref('core__dim_account') }}
    da
    ON A.creator_address = da.address
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
    CURRENT_DATE AS _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
