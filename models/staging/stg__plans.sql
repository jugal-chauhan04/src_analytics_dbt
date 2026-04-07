with staging_plans as(
    select 
        plan_id,
        product_id,
        plan_name,
        plan_price,
        recurring as billing_type
    from {{source('raw_src', 'plans')}}
)

select * from staging_plans