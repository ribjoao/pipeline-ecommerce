SELECT
	orw.order_id as order_id,
	o.order_purchase_timestamp as order_purchase_timestamp,
	review_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
FROM ecommerce.olist_order_reviews_dataset as orw
LEFT JOIN ecommerce.olist_orders_dataset as o
ON orw.order_id = o.order_id
WHERE order_purchase_timestamp::date = '{{ date_previous }}'