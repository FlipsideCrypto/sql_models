{{ config(
    materialized = 'view',
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_swaps']
) }}

SELECT
    'jupiter aggregator v2' AS swap_program,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint
FROM
    {{ ref('silver_solana__swaps_jupiter_dex') }}
