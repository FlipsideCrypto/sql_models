{{ config(
    warn_if = "> 10",
    severity = "warn"
) }}
{{ sequence_distinct_gaps(ref("thorchain__total_block_rewards"), "block_id") }}
