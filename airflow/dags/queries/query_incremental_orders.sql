SELECT *
FROM ecommerce.olist_orders_dataset
WHERE order_purchase_timestamp::date = '{{ date_previous }}'