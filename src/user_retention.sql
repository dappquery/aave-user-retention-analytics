SELECT "CM".cohort_number, "C"."start_timestamp","C"."end_timestamp","CBV"."new_signups",
sum(case when "CM"."week_number"=1 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 1",
sum(case when "CM"."week_number"=2 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 2",
sum(case when "CM"."week_number"=3 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 3",
sum(case when "CM"."week_number"=4 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 4",
sum(case when "CM"."week_number"=5 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 5",
sum(case when "CM"."week_number"=6 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 6",
sum(case when "CM"."week_number"=7 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 7",
sum(case when "CM"."week_number"=8 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 8",
sum(case when "CM"."week_number"=9 then (case when "CBV"."new_signups">0 then ((cast("CM"."active_users_making_txs" as float))/ "CBV"."new_signups") else
0 end)end) as "week 9"

FROM "aave_v2"."cohort_metric_values" as "CM" INNER JOIN "aave_v2"."cohorts" as "C" ON "CM"."cohort_number" = "C"."cohort_number" INNER JOIN "aave_v2"."cohort_base_values" AS "CBV" ON "CM"."cohort_number" = "CBV"."cohort_number"
WHERE "CM"."cohort_number" <= 10
group by "CM".cohort_number,"C"."start_timestamp","C"."end_timestamp","CBV"."new_signups"
ORDER BY "CM"."cohort_number" ;