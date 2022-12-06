{{ config(
    materialized = 'view',
    unique_key = '_unique_key'
) }}

SELECT
    'algomint' AS bridge,
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    bridger_address,
    bridge_address,
    direction,
    _inserted_timestamp
FROM
    {{ ref('silver__bridge_algomint') }}
UNION ALL
SELECT
    'glitter' AS bridge,
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    bridger_address,
    bridge_address,
    direction,
    _inserted_timestamp
FROM
    {{ ref('silver__bridge_glitter') }}
UNION ALL
SELECT
    'messina' AS bridge,
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    bridger_address,
    bridge_address,
    direction,
    _inserted_timestamp
FROM
    {{ ref('silver__bridge_messina') }}
UNION ALL
SELECT
    'milkomeda' AS bridge,
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    bridger_address,
    bridge_address,
    direction,
    _inserted_timestamp
FROM
    {{ ref('silver__bridge_milkomeda') }}
UNION ALL
SELECT
    'pnetwork' AS bridge,
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    bridger_address,
    bridge_address,
    direction,
    _inserted_timestamp
FROM
    {{ ref('silver__bridge_pnetwork') }}
UNION ALL
SELECT
    'portal' AS bridge,
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    bridger_address,
    bridge_address,
    direction,
    _inserted_timestamp
FROM
    {{ ref('silver__bridge_portal') }}
