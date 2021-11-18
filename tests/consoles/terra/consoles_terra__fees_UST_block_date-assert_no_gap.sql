{{ date_gaps(ref("consoles_terra__fees_UST"), [], "block_date") }}
--Removed partition by currency since some of the smaller currencies have verified date gaps - AM 11/12