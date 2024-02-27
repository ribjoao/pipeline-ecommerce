
with 

source as (

    select *
    from {{ source('ecommerce','order_items') }}
    where order_id is not null
    
),

order_items as (

    select

        cast(order_id as text) as order_id,
        cast(order_item_id as integer) as order_item_id,
    
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