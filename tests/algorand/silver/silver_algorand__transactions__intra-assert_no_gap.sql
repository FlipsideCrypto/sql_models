{{ delayed_sequence_gaps(ref('silver_algorand__transactions'), ["block_id","tx_id"], "intra", "_inserted_timestamp", "15 HOURS") }}
