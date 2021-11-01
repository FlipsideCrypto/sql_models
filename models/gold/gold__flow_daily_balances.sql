{{ config(materialized='incremental',
cluster_by=['date'], unique_key='date || node_id || delegator_id', tags=['gold','gold__flow_daily_balances','events', 'flow'])}}

WITH labels as (
	SELECT * FROM {{source('shared','udm_address_labels')}}
 WHERE blockchain = 'flow')
	SELECT
	balance_date as date,
	b.node_id,
	b.delegator_id,
	d.delegator_address as address,
    labels.l1_label as address_label_type,
    labels.l2_label as address_label_subtype,
    labels.project_name as address_label,
    labels.address_name as address_address_name,
	b.delegated_amount as balance,
	'FLOW' as currency,
	'staked' as balance_type
	FROM
        {{ source('flow', 'daily_staked_balances')}} b
	LEFT JOIN
        {{ source('gold','flow_delegator_addresses') }} d
	ON d.node_id = b.node_id
	AND d.delegator_id = b.delegator_id

    LEFT OUTER JOIN
        labels
    ON
        labels.address = d.delegator_address
	WHERE
	{% if is_incremental() %}
		date >= getdate() - interval '15 days'
	{% else %}
		date >= getdate() - interval '12  months'
	{% endif %}


	UNION

	SELECT
		date,
		NULL as node_id,
		NULL as delegator_id,
		balances.address,
        labels.l1_label as address_label_type,
        labels.l2_label as address_label_subtype,
        labels.project_name as address_label,
        labels.address_name as address_address_name,
		balance / 10e8,
		currency,
		balance_type
	FROM
        {{ source('flow', 'daily_balances')}} as balances

    LEFT OUTER JOIN
       labels
    ON
        labels.address = balances.address

	WHERE
	{% if is_incremental() %}
		date >= getdate() - interval '1 days'
	{% else %}
		date >= getdate() - interval '12  months'
	{% endif %}