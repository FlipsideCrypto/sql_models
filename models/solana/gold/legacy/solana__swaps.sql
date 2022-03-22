{{ config(
    materialized = 'view',
    persist_docs={"relation": true, "columns": true}, 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_swaps']
) }}

SELECT
    'jupiter aggregator v2' AS swap_program,
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
UNION
SELECT
    'orca' AS swap_program,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint
FROM
    {{ ref('silver_solana__swaps_orca_dex') }}
