{{
    config(
        materialized='incremental'
    )
}}

with 

source as (

    select *
    from {{ source('ecommerce','customers') }}
    {% if is_incremental() %}

    where order_purchase_timestamp::timestamp > (select max(order_purchase) from {{ this }})
    and customer_id is not null
    {% endif %}
    
),

customers as (

    select

    -- generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    
        cast(order_purchase_timestamp as timestamp) as order_purchase,
        cast(customer_id as text) as customer_id,
        cast(customer_zip_code_prefix as integer) as customer_zip_code_prefix,
        cast(customer_unique_id as text) as customer_unique_id,
        cast(customer_city as text) as customer_city,
        cast(customer_state as text) as customer_state

    from source
)

select * from customers
