with staging_discounts as(

    select 
        discount_id,
        discount_code,
        discount_type,
        discount_value,
        valid_from as discount_valid_from,
        valid_to as discount_valid_to,
        product_id,
        plan_id,
        is_recurring
    from {{source("raw_src", "discounts")}}
)

select * from staging_discounts