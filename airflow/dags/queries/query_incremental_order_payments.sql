SELECT
	op.order_id as order_id,
	o.order_purchase_timestamp as order_purchase_timestamp,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
FROM ecommerce.olist_order_payments_dataset as op
LEFT JOIN ecommerce.olist_orders_dataset as o
ON op.order_id = o.order_id
WHERE order_purchase_timestamp::date = '{{ date_previous }}'