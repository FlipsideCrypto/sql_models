{{ config(
    materialized = 'view',
    persist_docs={"relation": true, "columns": true}, 
    tags = ['snowflake', 'solana', 'silver_solana', 'solana_prices']
) }}

SELECT
    recorded_at,
    symbol,
    price,
    total_supply,
    volume_24h,
    provider
FROM
    {{ source(
        'shared',
        'prices_v2'
    ) }}
WHERE
    (
        provider = 'coinmarketcap'
        AND (
            asset_id = '5426'
            OR asset_id = '8526'
            OR asset_id = '11461'
            OR asset_id = '9549'
            OR asset_id = '12297'
            OR asset_id = '9015'
            OR asset_id = '7978'
            OR asset_id = '12236'
            OR asset_id = '11171'
            OR asset_id = '13524'
            OR asset_id = '6187'
            OR asset_id = '3408'
            OR asset_id = '825'
            OR asset_id = '7129'
            OR asset_id = '4195'
            OR asset_id = '11181'
            OR asset_id = '11212'
            OR asset_id = '11213'


            )
    )
    OR (
        provider = 'coingecko'
        AND (
            asset_id = 'solana'
            OR asset_id = 'cope'
            OR asset_id = 'bonfida'
            OR asset_id = 'jet'
            OR asset_id = 'mercurial'
            OR asset_id = 'mango-markets'
            OR asset_id = 'msol'
            OR asset_id = 'raydium'
            OR asset_id = 'saber'
            OR asset_id = 'solend'
            OR asset_id = 'serum'
            OR asset_id = 'lido-staked-sol'
            OR asset_id = 'ftx-token'
            OR asset_id = 'usd-coin'
            OR asset_id = 'tether'
            OR asset_id = 'terrausd'
            OR asset_id = 'star-atlas'
            OR asset_id = 'star-atlas-dao'
        )
    )
