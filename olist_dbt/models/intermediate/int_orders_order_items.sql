SELECT
    i.order_id,
    i.product_id,
    i.price,
    i.freight_value,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    cast(o.order_delivered_customer_date AS DATE) - cast(o.order_purchase_timestamp AS DATE) AS delivery_days
FROM {{ ref('stg_olist__order_items') }} AS i
JOIN {{ ref('stg_olist__orders')}} AS o ON i.order_id=o.order_id
WHERE o.order_status = 'delivered'