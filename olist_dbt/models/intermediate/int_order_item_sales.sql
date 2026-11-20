SELECT order_id, seller_id, price FROM {{ ref("stg_olist__order_items")}}
WHERE price IS NOT NULL