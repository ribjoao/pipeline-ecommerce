
with 

source as (

    select *
    from {{ source('ecommerce','order_payments') }}
    where order_id is not null
    
),

payments as (

    select

    -- generate surrogates key
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as payment_key,
        {{ dbt_utils.generate_surrogate_key(['order_id','payment_sequential']) }} as payment_sequential_key,
    
    -- keys and foreign keys
        cast(order_id as text) as order_id,
    
    -- payments features
        cast(payment_sequential as integer) as payment_sequential,
        cast(payment_type as text) as payment_type,
        cast(payment_installments as integer) as payment_installments,
        cast(payment_value as decimal) as payment_value

    from source
)

select * from payments





