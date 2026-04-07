{{ config(materialized='view') }}

with spine as(

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

    from 
        {{ ref('int__subscription_plan_join') }}
    cross join 
        unnest(
            generate_date_array(
                date_trunc(start_date, month),
                date_trunc(coalesce(cancel_date, end_date, current_date()), month),
                interval 1 month
                )
            ) as billing_month

),

invoices as (

    -- Import invoices and pre-compute invoice_month
    -- Isolating this here means if invoice logic changes
    -- there is one place to update it

    select
        subscription_id,
        invoice_id,
        invoice_date,
        amount_due,
        invoice_status,
        date_trunc(invoice_date, month) as invoice_month

    from {{ ref('stg__invoices') }}

),

joined as (


    select
        s.subscription_id,
        s.customer_id,
        s.plan_id,
        s.product_id,
        s.plan_name,
        s.plan_price,
        s.billing_type,
        s.billing_month,
        s.status,
        i.invoice_id,
        i.invoice_status,
        i.amount_due

    from spine as s
    left join invoices as i
        on s.subscription_id = i.subscription_id
        and (
            -- monthly: direct month match
            (
                s.billing_type = 'monthly'
                and i.invoice_month = s.billing_month
            )
            or
            -- yearly: match invoice to the correct subscription year
            (
                s.billing_type = 'yearly'
                and i.invoice_month = date_add(
                    date_trunc(s.start_date, month),
                    interval cast(
                        (date_diff(s.billing_month, date_trunc(s.start_date, month), month) / 12) * 12 as int64)
                    month
                )
            )
        )

),

final as( 

    -- Calculate monthly amounts for both Gross and Net MRR
    -- Both amounts live here so mart__gross_mrr and mart__net_mrr
    -- can both be built from this single spine without duplication
    --
    -- GROSS: derived from plan_price (before discounts)
    --   yearly plan_price is the annual amount so divide by 12
    --   monthly plan_price is already a monthly amount
    --
    -- NET: derived from amount_due on the invoice (post discount)
    --   same division logic applies for yearly invoices

    select
        subscription_id,
        customer_id,
        plan_id,
        product_id,
        plan_name,
        billing_type,
        billing_month,
        status,
        invoice_id,
        invoice_status,

        -- gross monthly amount: plan_price before any discounts
        case
            when billing_type = 'yearly'  then plan_price / 12
            when billing_type = 'monthly' then plan_price
        end as gross_monthly_amount,

        -- net monthly amount: invoiced amount after discounts applied
        case
            when billing_type = 'yearly'  then amount_due / 12
            when billing_type = 'monthly' then amount_due
        end as net_monthly_amount

    from joined

)
 
 
select * from final