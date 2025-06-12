
-- in Notebook this has to be installed. %pip install google-cloud-bigquery

-- 1.1. What are the top spending categories across companies?
-- query_1_1 = """
with 
a as 
(
select company_id as company_id, category, sum(billed_amount_aed) as total_amount_aed, count(1) number_of_trx 
from pemo-assessment-462223.pemo.transaction group by 1,2 order by 1,3 desc
),
b as 
(
select *, row_number () over (partition by company_id order by total_amount_aed desc) rn from a order by 1,3,2
)
select company_id, category as most_spending_category, total_amount_aed from b where rn = 1 
-- """

-- 1.2. Are there seasonal trends in spending?
-- query_1_2 ="""
select date_trunc(transaction_date,month) as year_month, count(1) cnt, sum(billed_amount_aed) total_amount_aed
from pemo-assessment-462223.pemo.transaction group by 1 order by 1
-- """


-- 1.3. Which companies have unusually high spending?
-- query_1_3 ="""
with 
company_profile as 
(
select company_id, count(1) cnt, sum(billed_amount_aed) total_amount_aed, avg(billed_amount_aed) avg_amount_aed 
from pemo-assessment-462223.pemo.transaction 
group by 1 order by 1
),
billed_delta as 
(
select t.*, cp.avg_amount_aed, cp.avg_amount_aed - billed_amount_aed as delta from pemo-assessment-462223.pemo.transaction t join company_profile cp on t.company_id = cp.company_id where t.company_id is not null
)
select company_id, count(1) cnt,
sum(billed_amount_aed) billed_amount_aed,
avg(avg_amount_aed) avg_amount_aed
from billed_delta where abs(delta) >= 2 * avg_amount_aed
group by 1

-- select 
-- case 

--   when abs(delta) / avg_amount_aed >= 2 and abs(delta) / avg_amount_aed < 3 then '2-3' 
--   when abs(delta) / avg_amount_aed >= 3 and abs(delta) / avg_amount_aed < 4 then '3-4'
--   when abs(delta) / avg_amount_aed >= 4 and abs(delta) / avg_amount_aed < 5 then '4-5'
--   when abs(delta) / avg_amount_aed >= 5 and abs(delta) / avg_amount_aed < 6 then '5-6'
--   else '6+'
-- end as delta_over_avg, 
-- count(1) cnt,
-- sum(billed_amount_aed) billed_amount_aed,
-- avg(avg_amount_aed) avg_amount_aed
-- -- count(1) 
-- from billed_delta where abs(delta) >= 2 * avg_amount_aed
-- group by 1


-- """

-- 2.1. Identify companies where a large percentage of transactions are in a foreign currency—could this indicate international misuse?
-- query_2_1 ="""
with 
a as 
(
select  company_id, 
        sum(case when currency != 'AED' then 0 else 1 end) as number_of_local_trx,
        sum(case when currency != 'AED' then 0 else billed_amount_aed end) as aed_amount_of_local_trx,
        sum(case when currency != 'AED' then 1 else 0 end) as number_of_foreign_trx,
        sum(case when currency != 'AED' then billed_amount_aed else 0 end) as aed_amount_of_foreign_trx
        -- count(1) cnt, sum(billed_amount_aed) billed_amount_aed 
from    pemo-assessment-462223.pemo.transaction
-- where currency != 'AED'
group by 1 
),
b as 
(
select  
case 
  when number_of_local_trx = 0 and number_of_foreign_trx > 100 then 'Super Risky | More than 100 transactions are foreign, no local transactions' 
  when number_of_local_trx >= 0 and number_of_local_trx < 10 and number_of_foreign_trx > 100 then 'Risky | More than 100 transactions are foreign, 0-10 local transactions' 
  when number_of_foreign_trx > 0 then 'Less Risky | At least 1 transaction in foreign exchange' 
else 'L' end as class, * from a
)
-- select count(1) cnt from a
select class, count(1) cnt from b group by 1
-- """

-- 2.2. Are there users transacting at a high rate with a single merchant?
-- query_2_2 ="""
with 
merchant_profile as 
(
select merchant, count(1) cnt, count(distinct user_id) number_of_users from pemo-assessment-462223.pemo.transaction group by 1
),
mp_2 as
(select merchant, cnt/number_of_users as avg_trx_per_user from merchant_profile),
user_merchant as 
(
select user_id, merchant, count(1) cnt from pemo-assessment-462223.pemo.transaction group by 1,2
),
a as 
(
select user_id, coalesce(a.merchant,b.merchant) merchant, avg_trx_per_user, cnt from user_merchant a left join mp_2 b on a.merchant = b.merchant
),
b as 
(
select case when cnt / avg_trx_per_user <  1.0 then 'OK | <1.0'
            when cnt / avg_trx_per_user >= 1.0 and cnt / avg_trx_per_user < 1.5 then 'OK | >=1.0-1.5'
            when cnt / avg_trx_per_user >= 1.5 and cnt / avg_trx_per_user < 2.0 then 'OKish | >=1.5-2.0'
            when cnt / avg_trx_per_user >= 2.0 and cnt / avg_trx_per_user < 2.5 then 'NOK | >=2.0-2.5' 
            when cnt / avg_trx_per_user >= 2.5 then 'NOK | >=2.5' 
            else null end 
            as compare, cnt / avg_trx_per_user as value,
            * from a
) 
-- select compare, count(1) cnt from b group by 1
select * from b where compare like 'NOK%' order by value desc 
-- """

-- Identify outlier transactions (e.g., transactions that are significantly larger than the norm) that are greater than 3x the interquartile range (IQR) from the median transaction value.
-- query_2_3 ="""
with 
percentiles as 
(
  select 
    approx_quantiles(billed_amount_aed, 100)[offset(25)] AS q1,
    approx_quantiles(billed_amount_aed, 100)[offset(75)] AS q3
  from  pemo-assessment-462223.pemo.transaction
),
limits as 
(
select  
      t.*,
      p.q1,
      p.q3,
      (p.q3 - p.q1) * 3           as iqr_range,
      p.q3 + (p.q3 - p.q1) * 3    as upper_limit
from  pemo-assessment-462223.pemo.transaction t
cross join percentiles p
)
select    *
from      limits
where     billed_amount_aed > upper_limit
order by  billed_amount_aed desc
-- """

-- 3.1. Based on spending patterns, which companies or industries could be ideal for upselling new financial products?
-- query_3_1 ="""
with 
-- max total transaction amount company
a as    
(select company_id, status, sum(billed_amount_aed) as total_amount_aed from pemo-assessment-462223.pemo.transaction group by 1,2 order by 1,2 desc),
b as 
(select * from a where status = 'APPROVED' order by 3 desc),
c as 
(select company_id, total_amount_aed from b order by 2 desc limit 20),
-- select company_id, count(1) cnt from b group by 1 having count(1) >1 
d as 

(
-- Most frequetly used category amounts
select company_id, sum(billed_amount_aed) total_amount_aed 
from pemo-assessment-462223.pemo.transaction where status = 'APPROVED' and category in 
(select category from (select category, sum(billed_amount_aed) total_amount_aed from pemo-assessment-462223.pemo.transaction where status = 'APPROVED' group by 1 order by 2 desc limit 1))
group by 1 order by 2 desc limit 20
),
e as 
(
select * from c union all
select * from d
)
select distinct company_id from e
-- """


-- 3.2. If Pemo were to offer discounts or rewards, which categories/merchants would be the best to target?
-- query_3_2 ="""
select      category, merchant, sum(billed_amount_aed) AS total_amount_aed, count(1) as cnt
from        pemo-assessment-462223.pemo.transaction
group by    category, merchant
order by    3 desc
limit 40
-- """