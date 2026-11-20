SELECT 
    extract(year from order_purchase_timestamp) AS order_purchase_year, 
    order_status, 
    customer_city, 
    customer_state, 
    COUNT(DISTINCT customer_unique_id) AS customer_count, 
    COUNT(DISTINCT order_id) AS order_count 
FROM {{ ref("int_orders_customers")}}
GROUP BY (order_purchase_year, order_status, customer_city, customer_state)


