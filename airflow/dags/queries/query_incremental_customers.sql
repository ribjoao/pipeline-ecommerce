SELECT
	c.customer_id as customer_id,
	o.order_purchase_timestamp as order_purchase_timestamp,
	customer_unique_id,
	customer_zip_code_prefix,
    customer_city,
	customer_state
FROM ecommerce.olist_customers_dataset as c
LEFT JOIN ecommerce.olist_orders_dataset as o
ON c.customer_id = o.customer_id
WHERE order_purchase_timestamp::date = '{{ date_previous }}'