{{ 
    config(
        materialized='table'
    ) 
}}

with mart_mrr as (

    select 
        billing_month,
        product_id,
        plan_id,
        plan_name,
        billing_type,
        sum(net_monthly_amount) as net_mrr
    from 
        {{ref('int__mrr_join') }}
    group by
        1,2,3,4,5
)

select * from mart_mrr
order by
        billing_month asc
