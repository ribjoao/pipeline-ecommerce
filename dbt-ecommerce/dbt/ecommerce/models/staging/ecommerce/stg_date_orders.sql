with 

source_purchase as (

    select order_id, order_purchase_timestamp
    from {{ source('ecommerce','orders') }}
    where order_id is not null
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