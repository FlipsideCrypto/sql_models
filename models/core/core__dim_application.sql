{{ config(
    materialized = 'incremental',
    unique_key = 'dim_application_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['app_id']
    ) }} AS dim_application_id,
    app_id,
    params,
    app_closed,
    COALESCE(
        da.dim_account_id,
        '-1'
    ) AS dim_account_id__creator,
    da.address AS creator_address,
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
    names.name AS app_name,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__application') }} A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.closed_at = b.block_id
    LEFT JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
    LEFT JOIN {{ ref('core__dim_account') }}
    da
    ON A.creator_address = da.address
    LEFT JOIN {{ ref('silver__application_names') }}
    names
    ON A.app_id = names.application_id

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
    OR app_id IN (
        SELECT
            app_id
        FROM
            {{ this }}
        WHERE
            dim_account_id__creator = '-1'
            OR dim_block_id__created_at = '-1'
    )
    OR names._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    '-1' AS dim_application_id,
    -1 AS app_id,
    NULL AS params,
    NULL AS app_closed,
    '-1' AS dim_account_id__creator,
    NULL AS creator_address,
    '-1' AS dim_block_id__created_at,
    NULL AS created_at,
    '-1' AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS app_name,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    '-2' AS dim_application_id,
    -2 AS app_id,
    NULL AS params,
    NULL AS app_closed,
    '-2' AS dim_account_id__creator,
    NULL AS creator_address,
    '-2' AS dim_block_id__created_at,
    NULL AS created_at,
    '-2' AS dim_block_id__closed_at,
    NULL AS closed_at,
    NULL AS app_name,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
