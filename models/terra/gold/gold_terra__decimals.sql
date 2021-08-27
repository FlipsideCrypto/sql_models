{{ config(
    materialized = 'incremental',
    unique_key = 'blockchain || asset_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['blockchain, 'asset_id'],
    tags = ['snowflake', 'terra_gold', 'terra_decimals']
) }}

SELECT
    *
FROM
    {{ source(
        'shared',
        'udm_decimal_adjustments'
    ) }}
WHERE
    blockchain = 'terra'
