{{ config(materialized="incremental", unique_key="date_day") }}

with
    source_date_day as (
        select
            date_day, date_actual, day_name, month_actual, year_actual, quarter_actual
        from {{ ref("dim_date") }}
        where date_day <= current_date()

    ),
    source_loan as (
        select
            date_day,
            date_actual,
            day_name,
            month_actual,
            year_actual,
            quarter_actual,
            count(distinct loan_id) as total_loans,
            count(
                distinct case when disbursement_date = date_day then loan_id end
            ) as total_loans_disbursed,
            count(
                distinct case when maturity_date = date_day then loan_id end
            ) as total_loans_matured,
            count(
                distinct case when loan_status = 'active' then loan_id end
            ) as total_loans_active,
            count(
                distinct case when loan_status = 'paid' then loan_id end
            ) as total_loans_paid,
            count(
                distinct case when loan_status = 'defaulted' then loan_id end
            ) as total_loans_defaulted
        from source_date_day d
        left outer join
            {{ ref("dim_loan") }} l
            on (l.dbt_valid_from <= d.date_day and l.dbt_valid_to >= d.date_day)
            and l.disbursement_date <= d.date_day
            and l.maturity_date >= d.date_day
        group by
            date_day, date_actual, day_name, month_actual, year_actual, quarter_actual
    ),

    final as (
        select
            date_day,
            date_actual,
            day_name,
            month_actual,
            year_actual,
            quarter_actual,
            total_loans,
            total_loans_disbursed,
            total_loans_active,
            total_loans_paid,
            total_loans_defaulted,
            total_loans_matured
        from source_loan
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
