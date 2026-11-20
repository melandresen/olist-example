SELECT 
    seller_id, 
    COUNT(order_id) AS order_count, 
    SUM(price) AS revenue_total
FROM {{ ref("int_order_item_sales")}}
GROUP BY seller_id
ORDER BY revenue_total DESC