{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'crosschain', 'address_labels', 'gold_address_labels'], 
) }}

SELECT 
    collection
    , mint
    , token_id
    , deal_score_rank
    , rarity_rank
    , floor_price
    , fair_market_price
    , price_low
    , price_high
FROM 
    {{ ref('silver_crosschain__nft_fair_market_value') }}