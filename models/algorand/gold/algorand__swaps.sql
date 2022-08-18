{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'algorand_swaps', 'gold'],
) }}

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
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_tinyman_dex') }}
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
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_algofi_dex') }}
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
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_pactfi_dex') }}
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
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_wagmiswap_dex') }}
UNION ALL
SELECT
    'humble swap' AS swap_program,
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
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_humble_swap_dex') }}
