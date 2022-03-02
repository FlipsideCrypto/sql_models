{{ config(
    materialized = 'view',
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_swaps']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint
FROM
    {{ ref('silver_solana__swaps_jupiter_dex') }}
