
with 

source as (

    select *
    from {{ source('ecommerce', 'products') }}
    where product_id is not null
),

products as (

    select

    --generate surrogate key
        {{dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

        cast(product_id as text) as product_id,
        cast(product_category_name as text) as product_category_name,
        cast(product_name_lenght as integer) as product_name_lenght,
        cast(product_description_lenght as integer) as product_description_lenght,
        cast(product_photos_qty as integer) as product_photos_qty,
        cast(product_weight_g as integer) as product_weight_g,
        cast(product_length_cm as integer) as product_length_cm,
        cast(product_height_cm as integer) as product_height_cm,
        cast(product_width_cm as integer) as product_width_cm

    from source
)

select * from products

    