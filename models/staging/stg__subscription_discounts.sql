with stage_subscription_discounts as(
    select
        sub_discount_id as subscription_discount_id,
        subscription_id,
        discount_id,
        applied_date,
        expiry_date

    from {{ source('raw_src', 'subscription_discounts') }}
)

select * from stage_subscription_discounts