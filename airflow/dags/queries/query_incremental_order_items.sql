	SELECT *
	FROM ecommerce.olist_order_items_dataset as oi
	LEFT JOIN ecommerce.olist_orders_dataset as o
    ON oi.order_id = o.order_id
	WHERE order_purchase_timestamp::date = '{{ date_previous }}'