-- Bottlenecks
-- Almost Full table scan
-- very unlikely for the historical data to be changed, maybe some new approved transaction which were not approved earlier
-- Assumption: each user can only be assigned to one company only, no need count(distinct)
-- Suggestion: Precalculation (dwh.agg_company_stats_monthly) + fresh data
-- Expected improvement: %95

-- Query to be checked

SELECT
c.company_id,
c.industry,
COUNT(DISTINCT u.user_id) AS active_users,
SUM(t.amount) AS total_spent,
EXTRACT(MONTH FROM t.transaction_date) AS txn_month
FROM transactions t
JOIN users u ON t.user_id = u.user_id
JOIN companies c ON u.company_id = c.company_id
WHERE t.transaction_date >= TIMESTAMP("2024-01-01")
AND t.status = 'APPROVED'
AND u.is_active = TRUE
GROUP BY c.company_id, c.industry, txn_month
ORDER BY total_spent DESC;

-- Reducing the Datasizes, Get Rid of costly count(distinct user_id)
-- Assumption: each user can only be assigned to one company only
-- Easy to read, easy to find the calculation problems if there is any.
-- with the assumption of the following values are unique and have no null values (primary_key)
      -- transactions.id
      -- users.user_id
      -- companies.company_id
with 
approved_transactions as
(
select    date_trunc(transaction_date, month)      as year_month,
          company_id      as company_id, 
          count(1)        as number_of_transactions, 
          sum(amount)     as total_spent 
from      transactions
where     transaction_date >= timestamp("2024-01-01") and
          status = 'APPROVED'
group by  1,2
),
company_users as 
(
select    u.company_id      as company_id, -- DQ: check NULL values, if so, might be a core system issue, some users did not assigned to a company
          c.industry        as industry,
          count(u.user_id)  as active_users -- get rid off DISTINCT which is costly
from      users u 
inner join companies c on c.company_id = u.company_id 
where     u.is_active = TRUE
group by  1,2
)
select    atrx.company_id, -- DQ: check NULL values, if there is, it means some transactions don't have company_id assigned or missing.
          industry,
          active_users,
          total_spent,
          year_month
from      approved_transactions as atrx 
left join company_users cu on cu.company_id = atrx.company_id -- DQ: check whether NULL - NULL matches exist
order by  total_spent desc

-- DWH Alternative 1 (very fast) Recommended -> 
-- Check Hypothetical pemo Data Lakehouse Model (Lake > Data Warehouse Model) to see data model
-- Generate a daily aggregation table in batch, till D-1 -> dwh.agg_company_stats_daily
-- Merge it with daily query for today only

MERGE dwh.agg_company_stats_monthly T
USING (
  -- This is query_2 (today only)
  WITH 
  approved_transactions AS (
    SELECT  
      DATE_TRUNC(transaction_date, MONTH) AS year_month,
      company_id, 
      COUNT(1) AS number_of_transactions, 
      SUM(amount) AS total_spent 
    FROM transactions
    WHERE transaction_date >= CURRENT_DATE
      AND status = 'APPROVED'
    GROUP BY 1, 2
  ),
  company_users AS (
    SELECT  
      u.company_id,
      c.industry,
      COUNT(u.user_id) AS active_users
    FROM users u 
    INNER JOIN companies c ON c.company_id = u.company_id 
    WHERE u.is_active = TRUE
    GROUP BY 1, 2
  )
  SELECT  
    atrx.company_id,
    cu.industry,
    cu.active_users,
    atrx.total_spent,
    atrx.year_month
  FROM approved_transactions atrx
  LEFT JOIN company_users cu ON cu.company_id = atrx.company_id
) AS S
ON T.company_id = S.company_id AND T.year_month = S.year_month

WHEN MATCHED THEN
  UPDATE SET 
    T.industry = S.industry,
    T.active_users = S.active_users,
    T.total_spent = S.total_spent

WHEN NOT MATCHED THEN
  INSERT (company_id, industry, active_users, total_spent, year_month)
  VALUES (S.company_id, S.industry, S.active_users, S.total_spent, S.year_month);


-- Datalake, NRT, loading DWH transaction table less than 10 mins can be costly
-- DWH 2 (relatively faster)
select    company_id, 
          company_industry                        as industry,
          count(distinct t.user_id)               as active_users,
          sum(amount_local)                       as total_spent,
          date_trunc(transaction_date, month)     as year_month
from      dwh.fact_transaction t
join      dwh.dim_user c on t.user_id = c.user_id and c.is_active = TRUE
where     t.status = 'APPROVED' and 
          transaction_date >= date('2024-01-01') 
group by  1,2,5
order by  4 desc

-- Add clustering for frequently used
alter table transactions
set options (
    clustering_fields = ["company_id", "status"]
);

alter table transactions
set options (
    clustering_fields = ["company_id", "is_active"]
);

-- Try using APPROX_COUNT_DISTINCT instead of count(distinct user_id)
-- This can be tried, if exact counts aren't critical.'

-- Materialized view to precompute aggregations for frequently accessed metrics, reducing query execution time.
CREATE MATERIALIZED VIEW monthly_metrics
PARTITION BY DATE(transaction_date)
CLUSTER BY company_id
AS
SELECT
    c.company_id,
    c.industry,
    COUNT(DISTINCT u.user_id) AS active_users,
    SUM(t.amount) AS total_spent,
    EXTRACT(MONTH FROM t.transaction_date) AS txn_month
FROM `transactions` t
JOIN `users` u
    ON t.user_id = u.user_id
    AND u.is_active = TRUE
JOIN `companies` c
    ON t.company_id = c.company_id
WHERE t.status = 'APPROVED'
GROUP BY c.company_id, c.industry, txn_month;


