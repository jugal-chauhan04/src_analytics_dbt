{{ 
    config(
        materialized='table'
    )
}}

with collected_payments as (

    select
        billing_month,
        product_id,
        plan_id,
        plan_name,
        billing_type,
        sum(collected_monthly_amount) as collected_mrr

    from {{ ref('int__subscription_payments_join') }}

    group by
        1,2,3,4,5

)

select * from collected_payments
order by
        billing_month desc