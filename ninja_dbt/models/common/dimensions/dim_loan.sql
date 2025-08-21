-- models/dim_customer.sql
{{ config(materialized="table", unique_key="loan_id") }}

with
    final as (
        select
            {{ dbt_utils.generate_surrogate_key(["loan_id"]) }} as loan_id
            , {{ dbt_utils.generate_surrogate_key(["application_id "]) }} as application_id 
            , {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id
            , cast(loan_id as varchar(255)) as SOURCE_APPROVED_LOAN_NUMBER
            , loan_amount as APPROVED_LOAN_AMOUNT
            , interest_rate as ANNUAL_INTEREST_RATE
            , start_date as DISBURSEMENT_DATE
            , end_date as MATURITY_DATE
            , status as LOAN_STATUS
            , row_number() over (
                partition by customer_id order by dbt_valid_from
            ) as version
            , case
                when dbt_valid_to is null then true else false
            end as is_current
            , dbt_updated_at
            , dbt_valid_from
            , case
                when dbt_valid_to is null then to_date('9999-12-31') 
            end as dbt_valid_to
        from {{ ref('snapshot_loans') }}
    )

{{ dbt_audit(
    cte_ref="final",
    created_by="@gitlabs",
    updated_by="@jniebuhr",
    created_date="2025-08-21",
    updated_date="2025-08-21"
) }}

