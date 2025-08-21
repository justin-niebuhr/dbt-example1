{{ config(materialized="table", unique_key="customer_id") }}

/*
Logic not handling historical records in the SCD2 "pre snapshot information"
*/

with
    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['application_id', 'dbt_valid_from']) }} as loan_application_SCD2_ID,
            {{ dbt_utils.generate_surrogate_key(["application_id"]) }} as application_id,
            {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id,
            cast(application_id as varchar(255)) as source_application_number,
            application_date as application_submit_date,
            loan_amount_requested as requested_loan_amount,
            {{ status_short("loan_applications", "status") }} as application_status,
            convert_timezone(
                get_current_timezone(), 'GMT', updated_at::timestamp_ntz
            )::timestamp_ntz as updated_at_gmt,
            convert_timezone(
                get_current_timezone(), updated_at::timestamp_ntz
            )::timestamp_ntz as updated_at_local,
            row_number() over (
                partition by customer_id order by dbt_valid_from
            ) as version,
            case when dbt_valid_to is null then true else false end as is_current,
            dbt_valid_from,
            case
                when dbt_valid_to is null then to_date('9999-12-31')
            end as dbt_valid_to
        from {{ ref("snapshot_loan_applications") }}
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
