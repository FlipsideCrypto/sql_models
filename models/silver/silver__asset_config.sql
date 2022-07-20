{{ config(
    materialized = 'incremental',
    unique_key = 'asset_id',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

SELECT
    asset_id,
    tx_message :txn :apar :an :: STRING AS asset_name,
    tx_message :txn :apar :t :: NUMBER AS asset_amount,
    CASE
        WHEN tx_message :txn :apar :dc :: NUMBER IS NULL THEN 0
        ELSE tx_message :txn :apar :dc :: NUMBER
    END AS decimals,
    MAX(_inserted_timestamp) _inserted_timestamp
FROM
    {{ ref('silver__transaction') }}
WHERE
    tx_type = 'acfg'
    AND tx_message :txn :apar :an :: STRING IS NOT NULL
    AND tx_message :txn :apar :t :: NUMBER IS NOT NULL
    AND tx_message :txn :apar IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
)
{% endif %}
GROUP BY
    asset_id,
    asset_name,
    asset_amount,
    decimals
