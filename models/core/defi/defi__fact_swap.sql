{{ config(
    materialized = 'incremental',
    unique_key = 'fact_swap_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        swap_program,
        block_id,
        intra,
        tx_group_id,
        app_id,
        swapper,
        swap_from_asset_id,
        swap_from_amount,
        pool_address,
        swap_to_asset_id,
        swap_to_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__swap') }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    ) - INTERVAL '4 HOURS'
    OR tx_group_id IN (
        SELECT
            tx_group_id
        FROM
            {{ this }}
        WHERE
            dim_block_id = '-1'
            OR dim_account_id__swapper = '-1'
            OR dim_asset_id__swap_from = '-1'
            OR dim_asset_id__swap_to = '-1'
            OR dim_application_id = '-1'
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.intra','a.swap_program']
    ) }} AS fact_swap_id,
    swap_program,
    COALESCE(
        f.block_timestamp,
        '1900-01-01' :: DATE
    ) block_timestamp,
    COALESCE(
        f.dim_block_id,
        '-1'
    ) AS dim_block_id,
    A.intra,
    A.tx_group_id,
    COALESCE(
        e.dim_application_id,
        '-1'
    ) AS dim_application_id,
    COALESCE(
        b.dim_account_id,
        '-1'
    ) AS dim_account_id__swapper,
    A.swapper,
    COALESCE(
        C.dim_asset_id,
        '-1'
    ) AS dim_asset_id__swap_from,
    A.swap_from_asset_id,
    A.swap_from_amount,
    A.pool_address,
    COALESCE(
        d.dim_asset_id,
        '-1'
    ) AS dim_asset_id__swap_to,
    A.swap_to_asset_id,
    A.swap_to_amount,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    LEFT JOIN {{ ref('core__dim_account') }}
    b
    ON A.swapper = b.address
    LEFT JOIN {{ ref('core__dim_asset') }} C
    ON A.swap_from_asset_id = C.asset_id
    LEFT JOIN {{ ref('core__dim_asset') }}
    d
    ON A.swap_to_asset_id = d.asset_id
    LEFT JOIN {{ ref('core__dim_application') }}
    e
    ON A.app_id = e.app_id
    LEFT JOIN {{ ref('core__dim_block') }}
    f
    ON A.block_id = f.block_id
