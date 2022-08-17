{{ config(
    materialized = 'incremental',
    unique_key = 'fact_account_application_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

WITH base AS (

    SELECT
        algorand_decode_hex_addr(
            addr :: text
        ) AS address,
        app :: INT AS app_id,
        deleted AS app_closed,
        closed_at AS closed_at,
        created_at AS created_at,
        localstate AS app_info,
        _inserted_timestamp
    FROM
        {{ ref('bronze__account_application') }}

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
    OR address || '--' || app_id IN (
        SELECT
            address || '--' || app_id
        FROM
            {{ this }}
        WHERE
            dim_account_id = '-1'
            OR dim_application_id = '-1'
            OR dim_block_id__created_at = '-1'
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.address','a.app_id']
    ) }} AS fact_account_application_id,
    COALESCE(
        da.dim_account_id,
        '-1'
    ) AS dim_account_id,
    A.address,
    COALESCE(
        dim_application_id,
        '-1'
    ) AS dim_application_id,
    A.app_id,
    A.app_closed,
    app_info,
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
    ON A.address = da.address
    LEFT JOIN {{ ref('core__dim_application') }}
    dap
    ON A.app_id = dap.app_id
