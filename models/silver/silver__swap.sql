{{ config(
    materialized = 'view',
    unique_key = '_unique_key'
) }}

SELECT
    'tinyman' AS swap_program,
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
    _unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_tinyman_dex') }}
UNION ALL
SELECT
    'algofi' AS swap_program,
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
    _unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_algofi_dex') }}
UNION ALL
SELECT
    'pactfi' AS swap_program,
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
    _unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_pactfi_dex') }}
UNION ALL
SELECT
    'wagmiswap' AS swap_program,
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
    _unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_wagmiswap_dex') }}
UNION ALL
SELECT
    'humbleswap' AS swap_program,
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
    _unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_humble_swap_dex') }}
UNION ALL
SELECT
    'fxdx' AS swap_program,
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
    _unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_fxdx_dex') }}
