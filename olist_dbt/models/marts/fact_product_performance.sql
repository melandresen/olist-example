with
    weight_photos_category as (
        select
            product_category_name,
            avg(product_weight_g) as average_product_weight,  -- Average product weight.
            avg(product_photos_qty) as average_count_photos  -- Average number of product photos.
        from {{ ref("stg_olist__products") }}
        where product_category_name is not null
        group by product_category_name
    ),

    sales_category as (
        select
            p.product_category_name,
            sum(o.price) as total_sales,  -- Total sales (sum of price).
            count(distinct o.product_id) as count_unique_products,  -- Number of unique products sold.
            count(distinct o.order_id) as count_orders,  -- Number of orders
            avg(o.delivery_days) as average_delivery_days,  -- Average delivery days.
        from {{ ref("int_orders_order_items") }} as o
        join {{ ref("stg_olist__products") }} as p on o.product_id = p.product_id
        where p.product_category_name is not null
        group by p.product_category_name
    )

select
    s.product_category_name as product_category_name_org,
    t.product_category_name_english as product_category_name,
    s.total_sales,
    s.count_unique_products,
    s.count_orders,
    s.average_delivery_days,
    w.average_product_weight,
    w.average_count_photos
from sales_category as s
join weight_photos_category as w on s.product_category_name = w.product_category_name
join
    {{ ref("stg_olist__product_category_name_translation") }} as t
    on s.product_category_name = t.product_category_name
order by s.total_sales desc