{{ 
    config(
        materialized='view'
        ) 
}}

with payments_aggregated as (

    -- Step 1: filter only successful payment attempts and collapse
    -- multiple attempts into one row per invoice
    -- a single invoice can have multiple payment attempts (retries)
    -- so we sum only successful amounts to get the true collected amount

    select
        invoice_id,
        sum(amount_paid) as collected_amount

    from {{ ref('stg__payments') }}

    where payment_status = 'success'

    group by invoice_id

),

payments_bridged as (

    -- Step 2: bridge payments to subscriptions via invoices
    -- payments only know about invoice_id
    -- we need subscription_id and invoice_date to connect to the spine
    -- invoice_date is also needed to determine which billing month
    -- this payment belongs to

    select
        p.invoice_id,
        i.subscription_id,
        i.invoice_date,
        i.invoice_status,
        p.collected_amount,
        date_trunc(i.invoice_date, month) as invoice_month

    from payments_aggregated as p
    inner join {{ ref('stg__invoices') }} as i
        on p.invoice_id = i.invoice_id

),
spine as (


    select
        subscription_id,
        customer_id,
        plan_id,
        product_id,
        plan_name,
        plan_price,
        billing_type,
        start_date,
        end_date,
        cancel_date,
        status,
        billing_month

    from {{ ref('int__subscription_plan_join') }}
    cross join unnest(
        generate_date_array(
            date_trunc(start_date, month),
            date_trunc(coalesce(cancel_date, end_date, current_date()), month),
            interval 1 month
        )
    ) as billing_month

),

final as (

    select
        s.subscription_id,
        s.customer_id,
        s.plan_id,
        s.product_id,
        s.plan_name,
        s.billing_type,
        s.billing_month,
        s.status,
        p.invoice_id,
        p.invoice_status,

        -- collected monthly amount: actual cash received post discount
        -- yearly collected amount divided by 12 to spread across active months
        -- monthly collected amount used as-is
        case
            when s.billing_type = 'yearly'  then p.collected_amount / 12
            when s.billing_type = 'monthly' then p.collected_amount
        end as collected_monthly_amount

    from spine as s
    left join payments_bridged as p
        on s.subscription_id = p.subscription_id
        and (
            (
                s.billing_type = 'monthly'
                and p.invoice_month = s.billing_month
            )
            or
            (
                s.billing_type = 'yearly'
                and p.invoice_month = date_add(
                    date_trunc(s.start_date, month),
                    interval cast(
                        (date_diff(s.billing_month, date_trunc(s.start_date, month), month) / 12) * 12
                    as int64) month
                )
            )
        )

)

select * from final