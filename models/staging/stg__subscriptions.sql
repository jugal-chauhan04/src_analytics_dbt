with stage_subscriptions as (
  select
    subscription_id,
    customer_id,
    plan_id,
    start_date,
    end_date,
    status,
    cancel_date
  from {{ source('raw_src', 'subscriptions') }}
)

select * from stage_subscriptions