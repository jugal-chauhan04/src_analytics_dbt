with staging_products as(
    select 
        product_id,
        product_name,
        product_description
    from {{ source("raw_src", "products") }}
)

select * from staging_products