SELECT o.order_id, o.order_status, o.order_purchase_timestamp, c.customer_unique_id, c.customer_city, c.customer_state FROM {{ ref('stg_olist__orders')}} AS o
JOIN {{ ref('stg_olist__customers')}} AS c ON o.customer_id=c.customer_id
WHERE o.order_status IN ('shipped','approved','delivered')