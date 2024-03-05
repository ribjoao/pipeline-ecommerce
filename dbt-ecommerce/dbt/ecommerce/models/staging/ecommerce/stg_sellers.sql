with 

source as (

    select *
    from {{ source('ecommerce', 'sellers') }}
    where seller_id is not null
),

sellers as (

    select

    -- generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,

        cast(seller_id as text) as seller_id,
        cast(seller_zip_code_prefix as integer) as seller_zip_code_prefix,
        cast(seller_city as text) as seller_city,
        cast(seller_state as text) as seller_state

    from source
)

select * from sellers