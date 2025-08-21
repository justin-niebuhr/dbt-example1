{% snapshot snapshot_loans %}

{{
    config(
        unique_key='loan_id',
        strategy='check',
        check_cols = ['customer_id',
                    'loan_amount',
                    'interest_rate',
                    'start_date',
                    'end_date',
                    'status',
                    ],

    )
}}

select * from {{ source('loan_source', 'loans') }}

{% endsnapshot %}