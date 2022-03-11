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
    from_asset_name,
    swap_from_asset_id,
    swap_from_amount,
    pool_address,
    to_asset_name,
    swap_to_asset_id,
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_tinyman_dex') }}
UNION
SELECT
    'algofi' AS swap_program,
    block_timestamp,
    block_id,
    intra,
    tx_group_id,
    app_id,
    swapper,
    from_asset_name,
    swap_from_asset_id,
    swap_from_amount,
    pool_address,
    to_asset_name,
    swap_to_asset_id,
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_algofi_dex') }}
UNION
SELECT
    'pactfi' AS swap_program,
    block_timestamp,
    block_id,
    intra,
    tx_group_id,
    app_id,
    swapper,
    from_asset_name,
    swap_from_asset_id,
    swap_from_amount,
    pool_address,
    to_asset_name,
    swap_to_asset_id,
    swap_to_amount
FROM
    {{ ref('silver_algorand__swaps_pactfi_dex') }}
