{% set silver_path = var('silver_path', '../data/silver/orders_clean_2018-01-02.csv') %}

{{ config(materialized='table') }}

with raw_orders as (
    select *
    from read_csv_auto('{{ silver_path }}')
)

select
    cast(id as integer) as order_id,
    cast(user_id as integer) as customer_id,
    cast(order_date as date) as order_date,
    lower(trim(status)) as status
from raw_orders
where order_id is not null
  and customer_id is not null
  and order_date is not null