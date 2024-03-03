{{ config(materialized='table')}}

select
-- surrogate key
    payment_key,

    payment_sequential_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value

from {{ ref('stg_order_payments') }}