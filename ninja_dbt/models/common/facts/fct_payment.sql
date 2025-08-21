-- models/dim_customer.sql
{{ config(materialized="incremental", unique_key="payment_id") }}

with
    snapshot_payments as (
        select
            {{ dbt_utils.generate_surrogate_key(["payment_id"]) }} as payment_id,
            cast(payment_id as varchar(255)) as source_payment_number,
            {{ dbt_utils.generate_surrogate_key(["loan_id"]) }} as loan_id,
            {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id,
            payment_amount,
            payment_date,
            {{ status_short("payments", "payment_type") }} as payment_type
        from {{ ref("snapshot_payments") }}
        where dbt_valid_to is null  -- get latest version of each row
    ),

    final as (
        select
            payment_id,
            source_payment_number,
            loan_id,
            customer_id,
            payment_amount,
            payment_date,
            payment_type
        from snapshot_payments
        {% if is_incremental() %}

            where payment_date >= (select max(payment_date) from {{ this }})
        {% endif %}
    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@gitlabs",
            updated_by="@jniebuhr",
            created_date="2025-08-21",
            updated_date="2025-08-21",
        )
    }}
