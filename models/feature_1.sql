
create table dbt_run.user_features_1 as(
select user_id,
Max(order_number) as user_orders,
Sum(days_since_prior_order) as user_period,
avg(days_since_prior_order) as user_mean_days_since_prior
from ori_arrow.RAW_DATA.ORDERS
group by user_id);
