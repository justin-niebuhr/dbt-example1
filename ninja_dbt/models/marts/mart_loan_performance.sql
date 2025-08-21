{{ config(materialized="incremental", unique_key="date_day") }}

with
    source_date_day as (
        select
            date_day,
            date_actual,
            day_name,
            month_actual,
            year_actual,
            quarter_actual
            from {{ ref("dim_date") }}
            Where date_day <= current_date()
            
    ),
    source_loan as (
        select
            date_day,
            date_actual,
            day_name,
            month_actual,
            year_actual,
            quarter_actual,
            COUNT(Distinct loan_id  ) as total_loans,
            COUNT(Distinct CASE WHEN DISBURSEMENT_DATE = date_day THEN loan_id END ) as total_loans_disbursed,
            COUNT(Distinct CASE WHEN MATURITY_DATE = date_day THEN loan_id END ) as total_loans_matured,
            COUNT(Distinct CASE WHEN loan_status = 'active' THEN loan_id END ) as total_loans_active,
            COUNT(Distinct CASE WHEN loan_status = 'paid' THEN loan_id END ) as total_loans_paid,
            COUNT(Distinct CASE WHEN loan_status = 'defaulted' THEN loan_id END ) as total_loans_defaulted
        from source_date_day d
        LEFT OUTER JOIN {{ ref("dim_loan") }} l on 
            (l.DBT_VALID_FROM <= d.date_day
            and l.DBT_VALID_TO >= d.date_day)
            and l.DISBURSEMENT_DATE <= d.date_day
            and l.MATURITY_DATE >= d.date_day
        group by 
            date_day,
            date_actual,
            day_name,
            month_actual,
            year_actual,
            quarter_actual   
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
