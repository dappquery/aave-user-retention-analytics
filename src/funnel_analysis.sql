select unnest(array['users_with_2plus_transactions','users_with_4plus_transactions','users_with_8plus_transactions', 'users_with_16plus_transactions', 'mini_whales_with_32plus_transactions', 'whales_with_32plus_transactions']) AS "Columns",
   unnest(array[users_with_2plus_transactions, users_with_4plus_transactions,users_with_8plus_transactions,users_with_16plus_transactions,mini_whales_with_32plus_transactions,whales_with_32plus_transactions]) AS "Values"
from
(
select
sum(case when temp_table1.user_count>=2 then 1 else 0 end) as "users_with_2plus_transactions",
sum(case when temp_table1.user_count>=4 then 1 else 0 end) as "users_with_4plus_transactions",
sum(case when temp_table1.user_count>=8 then 1 else 0 end) as "users_with_8plus_transactions",
sum(case when temp_table1.user_count>=16 then 1 else 0 end) as "users_with_16plus_transactions",
sum(case when temp_table1.user_count>=32 then 1 else 0 end) as "mini_whales_with_32plus_transactions",
sum(case when temp_table1.user_count>=64 then 1 else 0 end) as "whales_with_32plus_transactions"
from
(select "user",count(*) as user_count from aave_v2.user_transactions group by "user" having count(*)>1) temp_table1
) temp_table2;