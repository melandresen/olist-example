select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as datetime) AS shipping_limit_date,
    cast(price as NUMERIC(10,2)) AS price,
    cast(freight_value as NUMERIC(10,2)) as freight_value
from {{ source('olist', 'order_items')}}