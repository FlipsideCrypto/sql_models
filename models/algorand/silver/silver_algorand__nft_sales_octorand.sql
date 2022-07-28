{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge'
) }}

WITH gen1 AS (

    SELECT
        asset_id,
        1 AS gen1_placeholder
    FROM
        {{ ref('silver_algorand__asset') }}
    WHERE
        asset_name LIKE '%ctorand%'
        AND creator_address = 'X5YPUJ2HTFBY66WKWZOAA75WST5V7HWAGS2346SQFK622VNIRQ5ASXHTGA'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
gen2 AS (
    SELECT
        asset_id,
        1 AS gen2_placeholder
    FROM
        {{ ref('silver_algorand__asset') }}
    WHERE
        asset_name LIKE '%cto Prime%'
        AND creator_address IN (
            'XIUCOQPPZO2UNFD2TXQAEW7W5MPGZROVD2YUOGME22GNORYCJVMEYK3P5U',
            'UFFXUBZ5DFRLOQOB4LOC7GA3HTWMEEE54U3DJRTL27RKKV4UWOIID3I4FU',
            '6DGJ4FUQP623YFFIZXXOJ7OK63VILGT2FDGYCYI62VW2767DRBZFDTRMI4',
            'AB4T4VD7LRGHH75Z3KISVPNDENGY4W227RPAJEBYUDVKVNF2PWDKMHTO4A',
            'KPCXKFGBLR3WZN74BHG3RTKVOK6PW3UP53BHAYK7BLYDUCOTXJYKJU7JUY',
            'VOKX5CEPHTY6WJNZU4SQGCHCBK5MWNYXXIBUFQAMVTOCVP6VS6MFEEAFLM',
            'VVCR4Q2GYOQO3ENWQDQEFFGTNDJRA56QIYHUQ3RCZT36I6WXBAUU2FS7QE',
            'ZI35SDCVSLRTKUQWCA6SXYX2VUKDJ5JJEWDMDH6ZYMXTQBQDAE6GWUEU6I'
        )

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
raw_data AS (
    SELECT
        DISTINCT x.block_timestamp,
        x.block_id,
        x.tx_group_id,
        x.asset_receiver AS purchaser,
        x.asset_id AS nft_asset_id,
        x.asset_amount AS number_of_nfts,
        CASE
            WHEN x.asset_id IN (
                SELECT
                    asset_id
                FROM
                    gen1
            ) THEN 'gen1'
            WHEN x.asset_id IN (
                SELECT
                    asset_id
                FROM
                    gen2
            ) THEN 'gen2'
        END AS generation,
        SUM(
            y.amount
        ) AS total_sales_amount,
        MAX(
            x._INSERTED_TIMESTAMP
        ) AS _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__asset_transfer_transaction') }}
        x
        JOIN(
            SELECT
                asset_id
            FROM
                gen1
            UNION ALL
            SELECT
                asset_id
            FROM
                gen2
        ) nft
        ON x.asset_id = nft.asset_id
        JOIN {{ ref('silver_algorand__payment_transaction') }}
        y
        ON x.tx_group_id = y.tx_group_id
        JOIN (
            SELECT
                DISTINCT tx_group_id
            FROM
                {{ ref('silver_algorand__application_call_transaction') }}
        ) app_call
        ON x.tx_group_id = app_call.tx_group_id
    WHERE
        x.asset_amount > 0
        AND y.tx_message :txn :amt IS NOT NULL

{% if is_incremental() %}
AND x._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    x.block_timestamp,
    x.block_id,
    x.tx_group_id,
    x.asset_receiver,
    nft_asset_id,
    number_of_nfts,
    generation
)
SELECT
    rd.block_timestamp,
    rd.block_id,
    rd.tx_group_id,
    rd.purchaser,
    rd.nft_asset_id,
    CASE
        WHEN ast.decimals > 0 THEN number_of_nfts :: FLOAT / pow(
            10,
            ast.decimals
        )
        WHEN NULLIF(
            ast.decimals,
            0
        ) IS NULL THEN number_of_nfts :: FLOAT
    END AS number_of_nfts,
    rd.generation,
    rd.total_sales_amount / COUNT(1) over(
        PARTITION BY rd.tx_group_id
    ) AS total_sales_amount,
    concat_ws(
        '-',
        block_id :: STRING,
        tx_group_id :: STRING,
        rd.nft_asset_id :: STRING
    ) AS _unique_key,
    rd._INSERTED_TIMESTAMP
FROM
    raw_data rd
    LEFT JOIN {{ ref('silver_algorand__nft_asset') }}
    ast
    ON rd.nft_asset_id = ast.nft_asset_id
