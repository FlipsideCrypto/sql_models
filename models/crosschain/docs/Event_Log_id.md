{% docs cross_chain_event_log_id %}

[BETA TABLE] - This is the primary key for this table. This is a concatenation of the transaction hash and the event index at which the event occurred. This field can be used within other event based tables such as ```fact_transfers``` & ```ez_token_transfers```.

{% enddocs %}