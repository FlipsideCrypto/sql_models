{{ config(
    materialized = 'incremental',
    unique_key = 'dim_application_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

WITH base AS (

    SELECT
        INDEX AS app_id,
        algorand_decode_hex_addr(
            creator :: text
        ) AS creator_address,
        deleted AS app_closed,
        closed_at AS closed_at,
        created_at AS created_at,
        params,
        _inserted_timestamp
    FROM
        {{ ref('bronze__application') }}

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
        ['app_id']
    ) }} AS dim_application_id,
    app_id,
    params,
    app_closed,
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
    LEFT JOIN {{ ref('core__dim_account') }}
    da
    ON A.creator_address = da.address
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_application_id,
    NULL AS app_id,
    NULL AS params,
    NULL AS app_closed,
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
