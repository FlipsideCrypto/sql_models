{{ config(
    materialized = 'incremental',
    unique_key = 'fact_swap_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        'tinyman' AS swap_program,
        block_timestamp,
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
        {{ ref('silver__swaps_tinyman_dex') }}

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
{% endif %}
UNION ALL
SELECT
    'algofi' AS swap_program,
    block_timestamp,
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
    {{ ref('silver__swaps_algofi_dex') }}

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
{% endif %}
UNION ALL
SELECT
    'pactfi' AS swap_program,
    block_timestamp,
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
    {{ ref('silver__swaps_pactfi_dex') }}

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
{% endif %}
UNION ALL
SELECT
    'wagmiswap' AS swap_program,
    block_timestamp,
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
    {{ ref('silver__swaps_wagmiswap_dex') }}

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
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.intra','a.swap_program']
    ) }} AS fact_swap_id,
    swap_program,
    A.block_timestamp,
    A.block_id,
    A.intra,
    A.tx_group_id,
    e.dim_application_id,
    A.app_id,
    b.dim_account_id AS dim_account_id__swapper,
    A.swapper,
    C.dim_asset_id AS dim_asset_id__swap_from,
    A.swap_from_asset_id,
    A.swap_from_amount,
    A.pool_address,
    d.dim_asset_id AS dim_asset_id__swap_to,
    A.swap_to_asset_id,
    A.swap_to_amount,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    JOIN {{ ref('core__dim_account') }}
    b
    ON A.swapper = b.address
    JOIN {{ ref('core__dim_asset') }} C
    ON A.swap_from_asset_id = C.asset_id
    JOIN {{ ref('core__dim_asset') }}
    d
    ON A.swap_to_asset_id = d.asset_id
    JOIN {{ ref('core__dim_application') }}
    e
    ON A.app_id = e.app_id
