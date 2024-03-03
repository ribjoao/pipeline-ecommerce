
with 

source as (

    select *
    from {{ source('ecommerce','customers') }}
    where customer_id is not null
    
),

customers as (

    select

    -- generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    
    -- keys
        cast(customer_id as text) as customer_id,
        cast(customer_zip_code_prefix as integer) as customer_zip_code_prefix,
        cast(customer_unique_id as text) as customer_unique_id,
        cast(customer_city as text) as customer_city,
        cast(customer_state as text) as customer_state

    from source
)

select * from customers
