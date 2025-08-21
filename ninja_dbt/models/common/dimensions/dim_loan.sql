{{ config(materialized="table", unique_key="loan_id") }}

with
    final as (
        select
            {{ dbt_utils.generate_surrogate_key(["loan_id"]) }} as loan_id,
            {{ dbt_utils.generate_surrogate_key(["application_id "]) }}
            as application_id,
            {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id,
            cast(loan_id as varchar(255)) as source_approved_loan_number,
            loan_amount as approved_loan_amount,
            interest_rate as annual_interest_rate,
            start_date as disbursement_date,
            end_date as maturity_date,
            {{ status_short("loan", "status") }} as application_status,
            row_number() over (
                partition by customer_id order by dbt_valid_from
            ) as version,
            case when dbt_valid_to is null then true else false end as is_current,
            dbt_valid_from,
            case
                when dbt_valid_to is null then to_date('9999-12-31')
            end as dbt_valid_to
        from {{ ref("snapshot_loans") }}
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
