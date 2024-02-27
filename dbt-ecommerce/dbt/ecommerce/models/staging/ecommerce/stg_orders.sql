
with 

source as (

    select *
    from {{ source('ecommerce','orders') }}
    where order_id is not null
    
),

orders as (

    select
    
    -- keys and foreign keys
        cast(order_id as text) as order_id,
        cast(customer_id as text) as customer_id,
    
    -- order status
        cast(order_status as text) as order_status,

    -- order timestamps
        cast(order_purchase_timestamp as timestamp) as order_purchase,
        cast(order_approved_at as timestamp) as order_approved_at,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer,
        cast(order_estimated_delivery_date as timestamp) as order_delivered_estimated

    from source
)

select * from orders





