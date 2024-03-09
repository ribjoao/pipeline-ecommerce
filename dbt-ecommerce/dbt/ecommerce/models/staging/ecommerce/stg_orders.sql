{{
    config(
        materialized='incremental'
    )
}}

with 

source as (

    select *
    from {{ source('ecommerce','orders') }}
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses >= to include records whose timestamp occurred since the last run of this model)
    where order_purchase_timestamp::timestamp > (select max(order_purchase) from {{ this }})
    and order_id is not null
    {% endif %}
    
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