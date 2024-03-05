{{ config(materialized='table') }}

select
-- surrogate key
    seller_key,
    
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state

from {{ ref('stg_sellers') }}