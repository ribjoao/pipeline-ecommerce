{{
    config(
        materialized='incremental'
    )
}}

with 

source_purchase as (

    select order_id, order_purchase_timestamp
    from {{ source('ecommerce','orders') }}
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses >= to include records whose timestamp occurred since the last run of this model)
    where order_purchase_timestamp::timestamp > (select max(order_purchase) from {{ this }})
    and order_id is not null
    {% endif %}
),

date_orders_from_order_purchase as (

    select
    
    -- keys
        cast(order_purchase_timestamp as timestamp) as order_purchase,
        cast(order_id as text) as order_id,

    -- extract from timestamp
        cast(extract(day from order_purchase_timestamp::timestamp) as text) as day,
        cast(extract(month from order_purchase_timestamp::timestamp) as text) as month,
        cast(extract(year from order_purchase_timestamp::timestamp) as text)  as year,
        cast(extract(hour from order_purchase_timestamp::timestamp) as text) as hour,
        cast(extract(minute from order_purchase_timestamp::timestamp) as text) as minute,
        cast(extract(quarter from order_purchase_timestamp::timestamp) as text) as quarter,
        cast(extract(week from order_purchase_timestamp::timestamp) as text) as week,
        cast(extract(dow from order_purchase_timestamp::timestamp) as integer) + 1 as week_day

    from source_purchase
)

select * from date_orders_from_order_purchase