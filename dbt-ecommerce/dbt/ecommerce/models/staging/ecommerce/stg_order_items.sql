{{
    config(
        materialized='incremental'
    )
}}

with 

source as (

    select *
    from {{ source('ecommerce','order_items') }}
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses >= to include records whose timestamp occurred since the last run of this model)
    where order_purchase_timestamp::timestamp > (select max(order_purchase) from {{ this }})
    and order_id is not null
    {% endif %}
    
),

order_items as (

    select

        cast(order_id as text) as order_id,
        cast(order_item_id as integer) as order_item_id,
        cast(order_purchase_timestamp as timestamp) as order_purchase,
    -- foreign keys
        cast(product_id as text) as product_id,
        cast(seller_id as text) as seller_id,
    
    --timestamps
        cast(shipping_limit_date as timestamp) as shipping_limit,
    
    --order numeric values
        cast(price as numeric) as price,
        cast(freight_value as numeric) as freight_value

    from source
)

select * from order_items