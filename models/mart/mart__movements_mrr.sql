{{ 
config(
    materialized='table',
    tags=['mart']
)
}}

with mrr_movements as (

    select 
        billing_month,
        product_id,
        plan_id,
        billing_type,
        sum(case when movement_type = 'new_mrr' then movement_amount else 0 end) as new_mrr,
        sum(case when movement_type = 'churned_mrr' then movement_amount else 0 end) as churned_mrr,
        sum(case when movement_type = 'expansion_mrr' then movement_amount else 0 end) as expansion_mrr,
        sum(case when movement_type = 'contraction_mrr' then movement_amount else 0 end) as contraction_mrr

    from 
        {{ ref('int__mrr_movements') }}
    group by
        1,2,3,4
)
select 
        billing_month,
        product_id,
        plan_id,
        billing_type,
        new_mrr,
        churned_mrr,
        expansion_mrr,
        contraction_mrr,
        new_mrr + expansion_mrr - churned_mrr - contraction_mrr as net_mrr_movement
from mrr_movements
