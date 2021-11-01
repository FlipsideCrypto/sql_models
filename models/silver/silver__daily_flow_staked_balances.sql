{{ config(materialized='incremental',
cluster_by=['balance_date'],
  incremental_strategy = 'delete+insert',
unique_key='balance_date || node_id || delegator_id',
 tags=['gold', 'events','daily balance', 'flow','snowflake'])}}

  SELECT

            current_date()::timestamp as balance_date,
            node_id,
            delegator_id::integer as delegator_id,
            (CASE WHEN sum(event_amount) / 10e8 < 0
            THEN 0
            ELSE sum(event_amount) / 10e8 END) as delegated_amount
        FROM (
            SELECT
                blockchain,
                block_timestamp,
                block_id,
                event_from as delegator_id,
                event_to as node_id,
                event_amount
            FROM
               {{ source('flow', 'udm_events_flow')}}
            WHERE event_type = 'delegator_tokens_committed'

            UNION

            SELECT
                blockchain,
                block_timestamp,
                block_id,
                event_to as delegator_id,
                event_from as node_id,
                (event_amount * -1)
            FROM
                {{ source('flow', 'udm_events_flow')}}
            WHERE event_type = 'delegator_tokens_unstaked'

            UNION

            SELECT
                blockchain,
                block_timestamp,
                block_id,
                '0' as delegator_id,
                event_to as node_id,
                (event_amount * -1)
            FROM
                {{ source('flow', 'udm_events_flow')}}
            WHERE event_type = 'tokens_unstaked'

            UNION


            SELECT
                blockchain,
                block_timestamp,
                block_id,
                '0'as delegator_id,
                event_to as node_id,
                event_amount
            FROM
                {{ source('flow', 'udm_events_flow')}}
            WHERE event_type = 'tokens_staked'

        )
        WHERE block_timestamp < current_date()::timestamp + interval '1 day'
        GROUP BY 1,2,3,4