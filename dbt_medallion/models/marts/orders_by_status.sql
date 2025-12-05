{{ config(materialized='table') }}

with orders as (
    select *
    from {{ ref('stg_orders') }}
)

select
    order_date,
    status,
    count(*) as total_orders
from orders
group by 1, 2
order by order_date, status