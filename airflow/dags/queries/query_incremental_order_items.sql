SELECT
	oi.order_id as order_id,
	o.order_purchase_timestamp as order_purchase_timestamp,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
FROM ecommerce.olist_order_items_dataset as oi
LEFT JOIN ecommerce.olist_orders_dataset as o
ON oi.order_id = o.order_id
WHERE order_purchase_timestamp::date = '{{ date_previous }}'