{{ config(materialized='incremental')}}

with 

orders as (

    select *
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}

    where order_purchase > (select max(order_purchase) from {{ this }})
    {% endif %}
),

order_items as (

    select *
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}

    where order_purchase > (select max(order_purchase) from {{ this }})
    {% endif %}
),

joined_fact as (

    select
        -- surrogate keys
        {{ dbt_utils.generate_surrogate_key(['orders.order_id','order_item_id']) }} as order_item_key,
        {{ dbt_utils.generate_surrogate_key(['orders.order_purchase']) }} as order_date_key,
        {{ dbt_utils.generate_surrogate_key(['orders.customer_id']) }} as customer_key,
        {{ dbt_utils.generate_surrogate_key(['order_items.product_id']) }} as product_key,
        {{ dbt_utils.generate_surrogate_key(['order_items.seller_id']) }} as seller_key,
        
        -- keys
        orders.order_id as order_id,
        order_items.order_item_id as order_item_id,
    
        -- features
        orders.order_purchase as order_purchase,
        orders.order_status as order_status,
        order_items.price as price,
        order_items.freight_value as freight_value

    from orders

    left join order_items on orders.order_id = order_items.order_id

)

select * from joined_fact
