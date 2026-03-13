with stage_line_items as(
    select
        line_item_id,
        invoice_id,
        plan_id,
        amount as balance_adjustment,
        description as adjustment_description,
        line_type

    from {{ source('raw_src', 'line_items') }}
)

select * from stage_line_items