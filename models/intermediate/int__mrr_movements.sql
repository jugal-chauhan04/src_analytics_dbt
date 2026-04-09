-- New MRR: status = cancelled & start_month = billing_month & no prior sibscription exist for the customer

{{ 
    config(
        materialized='table'
    ) 
}}

with sub_plan as(

    select 
        customer_id,
        subscription_id, 
        plan_id, 
        start_date,
        end_date,
        cancel_date,
        status, 
        product_id,
        plan_price,
        plan_name,
        billing_type
    from 
        {{ ref('int__subscription_plan_join')}}
),

new_mrr as ( 

    select 
        p.customer_id,
        p.subscription_id, 
        p.plan_id, 
        date_trunc(p.start_date, month) as billing_month,
        p.product_id,
        p.plan_name,
        p.billing_type,
        'new_mrr' as movement_type,
        case 
            when p.billing_type = 'yearly' then p.plan_price / 12
            else p.plan_price
        end as movement_amount

    from sub_plan as p 
    left join sub_plan as prev
        on p.customer_id = prev.customer_id
        and p.start_date = prev.end_date 
    where prev.subscription_id is null
),

churned_mrr as ( 

    select 
        customer_id,
        subscription_id,
        plan_id,
        date_trunc(cancel_date, month) as billing_month,
        product_id,
        plan_name,
        billing_type,
        'churned_mrr' as movement_type,
        case   
            when billing_type = 'yearly' then plan_price / 12
            else plan_price
        end as movement_amount
    from 
        sub_plan
    
    where status = 'cancelled'
),

expansion_mrr as ( 

    select 
        s.customer_id,
        s.subscription_id, 
        s.plan_id, 
        date_trunc(prev.end_date, month) as billing_month,
        s.product_id,
        s.plan_name,
        s.billing_type,
        'expansion_mrr' as movement_type,
        case 
            when s.billing_type = 'yearly' then (s.plan_price - prev.plan_price) / 12
            else (s.plan_price - prev.plan_price)
        end as movement_amount
    from sub_plan s 
    join sub_plan prev
        on s.customer_id = prev.customer_id
        and s.start_date = prev.end_date 
    where s.plan_price > prev.plan_price and prev.status = 'upgraded'
),

contraction_mrr as (

    select 
        s.customer_id,
        s.subscription_id, 
        s.plan_id, 
        date_trunc(prev.end_date, month) as billing_month,
        s.product_id,
        s.plan_name,
        s.billing_type,
        'contraction_mrr' as movement_type,
        case 
            when s.billing_type = 'yearly' then (prev.plan_price - s.plan_price) / 12
            else (prev.plan_price - s.plan_price)
        end as movement_amount
    from sub_plan s 
    join sub_plan prev
        on s.customer_id = prev.customer_id
        and s.start_date = prev.end_date 
    where s.plan_price < prev.plan_price and prev.status = 'downgraded'
),

final as (

    select * from new_mrr
    union all
    select * from churned_mrr
    union all
    select * from expansion_mrr
    union all
    select * from contraction_mrr
)

select * from final