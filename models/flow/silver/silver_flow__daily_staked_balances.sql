{{ config(
    materialized = 'incremental',
    cluster_by = ['balance_date'],
    incremental_strategy = 'delete+insert',
    unique_key = "CONCAT_WS('-',balance_date, node_id, delegator_id)",
    tags = ['silver', 'events','daily staked balance', 'flow', 'snowflake']
) }}

SELECT
    *
FROM
    (
        SELECT
            CURRENT_DATE() :: TIMESTAMP AS balance_date,
            node_id,
            delegator_id :: INTEGER AS delegator_id,
            (
                CASE
                    WHEN SUM(event_amount) / 10e8 < 0 THEN 0
                    ELSE SUM(event_amount) / 10e8
                END
            ) AS delegated_amount
        FROM
            (
                SELECT
                    *
                FROM
                    (
                        SELECT
                            blockchain,
                            block_timestamp,
                            block_id,
                            event_from AS delegator_id,
                            event_to AS node_id,
                            event_amount
                        FROM
                            {{ source(
                                'flow',
                                'udm_events_flow'
                            ) }}
                        WHERE
                            event_type = 'delegator_tokens_committed'
                        UNION
                        SELECT
                            blockchain,
                            block_timestamp,
                            block_id,
                            event_to AS delegator_id,
                            event_from AS node_id,
                            (
                                event_amount * -1
                            ) AS event_amount
                        FROM
                            {{ source(
                                'flow',
                                'udm_events_flow'
                            ) }}
                        WHERE
                            event_type = 'delegator_tokens_unstaked'
                        UNION
                        SELECT
                            blockchain,
                            block_timestamp,
                            block_id,
                            '0' AS delegator_id,
                            event_to AS node_id,
                            (
                                event_amount * -1
                            ) AS event_amount
                        FROM
                            {{ source(
                                'flow',
                                'udm_events_flow'
                            ) }}
                        WHERE
                            event_type = 'tokens_unstaked'
                        UNION
                        SELECT
                            blockchain,
                            block_timestamp,
                            block_id,
                            '0' AS delegator_id,
                            event_to AS node_id,
                            event_amount
                        FROM
                            {{ source(
                                'flow',
                                'udm_events_flow'
                            ) }}
                        WHERE
                            event_type = 'tokens_staked'
                    )
                WHERE
                    block_timestamp < CURRENT_DATE() :: TIMESTAMP + INTERVAL '1 day'
            )
        GROUP BY
            1,
            2,
            3
    )
GROUP BY
    1,
    2,
    3,
    4
