SELECT
    product_id,
    product_category_name,
    cast(product_name_lenght AS INT) AS product_name_length,
    cast(product_description_lenght AS INT) AS product_description_length,
    cast(product_photos_qty AS INT) AS product_photos_qty,
    cast(product_weight_g AS INT) AS product_weight_g,
    cast(product_length_cm AS INT) AS product_length_cm,
    cast(product_height_cm AS INT) AS product_height_cm,
    cast(product_width_cm AS INT) AS product_width_cm
FROM {{ source('olist', 'products') }}