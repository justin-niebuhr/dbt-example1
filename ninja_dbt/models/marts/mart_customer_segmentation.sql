{{ config(materialized="incremental", unique_key="date_day") }}

with
    source_customer as (
        select customer_id, email, first_name, last_name from {{ ref("dim_customer") }} where is_current
    ),
    source_loan_application as (
        select customer_id, max(APPLICATION_SUBMIT_DATE) as last_application_date ,count(APPLICATION_ID) as loan_application_count from {{ ref("dim_loan_application") }} where is_current GROUP BY customer_id
    ),
    source_last_payment as (
        select
            customer_id,
            payment_date as last_payment_date,
            payment_type as last_payment_type,
            payment_amount as last_payment_amount
        from  {{ ref("fct_payment") }} p 
        qualify row_number() over (partition by customer_id order by payment_date desc) = 1
    ),
    source_loan as (
        select
            customer_id,
            count(distinct loan_id) as total_loans,
            count(
                distinct case when loan_status = 'active' then loan_id end
            ) as total_loans_active,
            count(
                distinct case when loan_status = 'paid' then loan_id end
            ) as total_loans_paid,
            count(
                distinct case when loan_status = 'defaulted' then loan_id end
            ) as total_loans_defaulted,
            sum(
                case when loan_status = 'active' then approved_loan_amount else 0 end
            ) as total_active_loan_amount
        from {{ ref("dim_loan") }}
        group by customer_id
    ),

    final as (
        select
            c.customer_id,
            email,
            first_name,
            last_name,
            last_application_date,
            loan_application_count,
            total_loans,
            total_loans_active,
            total_loans_paid,
            total_loans_defaulted,
            last_payment_date,
            last_payment_type,
            last_payment_amount
        from source_customer c
        left outer join source_loan l on c.customer_id = l.customer_id
        left outer join source_last_payment p on c.customer_id = p.customer_id
        left outer join  source_loan_application a on c.customer_id = a.customer_id
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
