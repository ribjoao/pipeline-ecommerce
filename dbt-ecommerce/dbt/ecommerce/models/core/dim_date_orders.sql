{{ config(materialized='table')}}

select
    {{ dbt_utils.generate_surrogate_key(['order_purchase']) }} as order_date_key,
    order_purchase,
    order_id,
    day,
    month,
    year,
    hour,
    minute,
    quarter,
    week,
    week_day

from {{ ref('stg_date_orders') }}