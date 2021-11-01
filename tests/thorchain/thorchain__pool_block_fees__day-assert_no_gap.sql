{{ date_gaps(ref("thorchain__pool_block_fees"), ["pool_name"], "day", dict(start_date = "cast('2021-07-22' as date)", end_date = "cast('2021-08-11' as date)")) }}
