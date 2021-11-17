{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['date'],
    unique_key = "CONCAT_WS('-', date, node_id, address, delegator_id)",
    tags = ['gold','gold__flow_daily_balances','events', 'flow']
) }}

WITH labels AS (

    SELECT
        *
    FROM
        {{ source(
            'shared',
            'udm_address_labels'
        ) }}
    WHERE
        blockchain = 'flow'
)
SELECT
    balance_date AS DATE,
    b.node_id,
    b.delegator_id,
    d.delegator_address AS address,
    labels.l1_label AS address_label_type,
    labels.l2_label AS address_label_subtype,
    labels.project_name AS address_label,
    labels.address_name AS address_address_name,
    b.delegated_amount AS balance,
    'FLOW' AS currency,
    'staked' AS balance_type
FROM
    {{ ref(
        'silver_flow__daily_staked_balances'
    ) }}
    b
    LEFT JOIN {{ ref(
        'gold__flow_delegator_addresses'
    ) }}
    d
    ON d.node_id = b.node_id
    AND d.delegator_id = b.delegator_id
    LEFT OUTER JOIN labels
    ON labels.address = d.delegator_address
WHERE

{% if is_incremental() %}
DATE >= getdate() - INTERVAL '15 days'
{% else %}
    DATE >= getdate() - INTERVAL '12 months'
{% endif %}
UNION
SELECT
    DATE,
    NULL AS node_id,
    NULL AS delegator_id,
    balances.address,
    labels.l1_label AS address_label_type,
    labels.l2_label AS address_label_subtype,
    labels.project_name AS address_label,
    labels.address_name AS address_address_name,
    balance / 10e8,
    currency,
    balance_type
FROM
    {{ ref(
        'silver_flow__daily_balances'
    ) }} AS balances
    LEFT OUTER JOIN labels
    ON labels.address = balances.address
WHERE

{% if is_incremental() %}
DATE >= getdate() - INTERVAL '1 days'
{% else %}
    DATE >= getdate() - INTERVAL '12 months'
{% endif %}
