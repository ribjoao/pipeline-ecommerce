{{ config(materialized='table')}}

with 

orders as (

    select *
    from {{ ref('stg_orders') }}
),

order_items as (

    select *
    from {{ ref('stg_order_items') }}
),

joined_fact as (

    select
        -- surrogate keys
        {{ dbt_utils.generate_surrogate_key(['orders.order_id','order_item_id']) }} as order_key,
        {{ dbt_utils.generate_surrogate_key(['order_purchase']) }} as order_date_key,
        {{ dbt_utils.generate_surrogate_key(['orders.order_id']) }} as payment_key,
        
        -- keys
        orders.order_id as order_id,
        order_items.order_item_id as order_item_id,
    
        -- features
        orders.order_purchase as order_purchase,
        orders.order_status as order_status,
        order_items.price as price,
        order_items.freight_value as freight_value

    from orders

    join order_items on orders.order_id = order_items.order_id

)

select * from joined_fact
