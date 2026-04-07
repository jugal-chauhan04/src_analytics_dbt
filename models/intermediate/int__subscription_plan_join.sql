

{{config(
    materialized = 'view'
)}}

with sub_plan_join as(
    select 
        s.customer_id,
        s.subscription_id, 
        s.plan_id, 
        s.start_date, 
        s.end_date, 
        s.cancel_date,
        s.status, 
        p.product_id,
        p.plan_price,
        p.plan_name,
        p.billing_type
    from 
        {{ ref('stg__subscriptions')}} as s 
        join {{ ref('stg__plans')}} as p 
        on s.plan_id = p.plan_id
)

select * from sub_plan_join