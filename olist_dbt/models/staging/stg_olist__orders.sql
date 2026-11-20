select 
    order_id as order_id,
    customer_id as customer_id,
    order_status as order_status,
    cast(order_purchase_timestamp as datetime) as order_purchase_timestamp,
    cast(order_approved_at as datetime) as order_approved_at,
    cast(order_delivered_carrier_date as datetime) as order_delivered_carrier_date,
    cast(order_delivered_customer_date as datetime) as order_delivered_customer_date,
    cast(order_estimated_delivery_date as date) as order_estimated_delivery_date
from {{ source('olist', 'orders')}}