{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', blockchain, symbol, address, xfer_date)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'ntr', 'crosschain']
) }}

SELECT
    (
        record_metadata :CreateTime :: INT / 1000
    ) :: timestamp_ntz AS system_created_at,
    VALUE :blockchain :: STRING AS blockchain,
    VALUE :symbol :: STRING AS symbol,
    VALUE :address :: STRING AS address,
    VALUE :date :: DATE AS xfer_date,
    VALUE :reward :: FLOAT AS reward,
    VALUE :hodl :: FLOAT AS hodl,
    VALUE :unlabeled_transfer :: FLOAT AS unlabeled_transfer,
    VALUE :stake :: FLOAT AS stake,
    VALUE :cex_deposit :: FLOAT AS cex_deposit,
    VALUE :nft_buy :: FLOAT AS nft_buy,
    VALUE :dex_swap :: FLOAT AS dex_swap,
    VALUE :bridge :: FLOAT AS bridge,
    TO_BOOLEAN(LOWER(VALUE :first_is_bounty :: STRING)) AS first_is_bounty,
    TO_BOOLEAN(LOWER(VALUE :did_hunt :: STRING)) AS did_hunt,
    TO_BOOLEAN(LOWER(VALUE :did_new_user :: STRING)) AS did_new_user,
    TO_BOOLEAN(LOWER(VALUE :did_bounty :: STRING)) AS did_bounty
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    *
                FROM
                    {{ source(
                        'bronze',
                        'prod_data_science_uploads_1748940988'
                    ) }}
                WHERE
                    TRIM(
                        record_metadata :key :: STRING,
                        '"'
                    ) LIKE 'ntr%'
            ),
            LATERAL FLATTEN(
                input => record_content
            ) AS f
    )
WHERE
    xfer_date IS NOT NULL

{% if is_incremental() %}
AND (
    record_metadata :CreateTime :: INT / 1000
) :: timestamp_ntz :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(system_created_at :: DATE))
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY blockchain, symbol, address, xfer_date
ORDER BY
    system_created_at DESC)) = 1
