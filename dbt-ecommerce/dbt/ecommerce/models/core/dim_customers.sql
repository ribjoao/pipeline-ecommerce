{{ config(materialized='table')}}

select
-- surrogate key
    customer_key,

    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

from {{ ref('stg_customers') }}