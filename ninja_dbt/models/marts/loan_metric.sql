{{ config(materialized="incremental", unique_key="date_day") }}

with
    source_date_day as (
        select date_day, date_actual, month_actual, year_actual, quarter_actual
        from {{ ref("dim_date") }}
        where date_day <= current_date()

    ),
    source_payment as (
        select
            payment_date as date_day,
            sum(
                case when payment_type = 'scheduled' then else 0 end
            ) as total_scheduled_payment_amount,
            sum(
                case when payment_type = 'prepayment' then else 0 end
            ) as total_prepayment_payment_amount,
            sum(payment_amount) as total_payment_amount
        from {{ ref("fct_payment") }}
        group by payment_date
    ),
    source_loan as (
        select
            date_day,
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
        group by date_day
    ),
    slice_time as (
        select
            'year' as time_slice,
            cast(year_actual as varchar(20)) as time_value,
            total_loans,
            total_loans_disbursed,
            total_loans_matured,
            total_loans_active,
            total_loans_paid,
            total_loans_defaulted,
            total_scheduled_payment_amount,
            total_prepayment_payment_amount,
            total_payment_amount
        from source_date_day d
        left outer join source_loan l on d.date_day = l.date_day
        left outer join source_payment p on d.date_day = p.date_day
        group by year_actual
        union all
        select
            'quarter' as time_slice,
            cast(quarter_actual as varchar(20)) as time_value,
            total_loans,
            total_loans_disbursed,
            total_loans_matured,
            total_loans_active,
            total_loans_paid,
            total_loans_defaulted,
            total_scheduled_payment_amount,
            total_prepayment_payment_amount,
            total_payment_amount
        from source_date_day d
        left outer join source_loan l on d.date_day = l.date_day
        left outer join source_payment p on d.date_day = p.date_day
        group by quarter_actual
        union all
        select
            'month' as time_slice,
            cast(month_actual as varchar(20)) as time_value,
            total_loans,
            total_loans_disbursed,
            total_loans_matured,
            total_loans_active,
            total_loans_paid,
            total_loans_defaulted,
            total_scheduled_payment_amount,
            total_prepayment_payment_amount,
            total_payment_amount
        from source_date_day d
        left outer join source_loan l on d.date_day = l.date_day
        left outer join source_payment p on d.date_day = p.date_day
        group by month_actual
        union all
        select
            'day' as time_slice,
            cast(date_actual as varchar(20)) as time_value,
            total_loans,
            total_loans_disbursed,
            total_loans_matured,
            total_loans_active,
            total_loans_paid,
            total_loans_defaulted,
            total_scheduled_payment_amount,
            total_prepayment_payment_amount,
            total_payment_amount
        from source_date_day d
        left outer join source_loan l on d.date_day = l.date_day
        left outer join source_payment p on d.date_day = p.date_day
        group by date_actual
    ),
    final as (select * from slice_time)

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@gitlabs",
            updated_by="@jniebuhr",
            created_date="2025-08-21",
            updated_date="2025-08-21",
        )
    }}
