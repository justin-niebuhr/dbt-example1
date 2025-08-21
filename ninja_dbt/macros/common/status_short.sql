{%- macro status_short(source_table, status_name) -%}
  {# This may be tied to an MDM platform depending on needs #}
CASE
  WHEN LOWER('{{ source_table }}') LIKE '%loan_applications%' THEN 
    CASE
        WHEN LOWER('{{ status_name }}') LIKE '%pending%' THEN 'Pending'
        WHEN LOWER('{{ status_name }}') LIKE '%approved%' THEN 'Approved'
        WHEN LOWER('{{ status_name }}') LIKE '%rejected%' THEN 'Rejected'
        ELSE 'Other Loan Application Status'
    END
  WHEN LOWER('{{ source_table }}') LIKE '%loans%' THEN 
    CASE
        WHEN LOWER('{{ status_name }}') LIKE '%active%' THEN 'Active'
        WHEN LOWER('{{ status_name }}') LIKE '%paid%' THEN 'Paid'
        WHEN LOWER('{{ status_name }}') LIKE '%defaulted%' THEN 'Defaulted'
        ELSE 'Other Loan Status'
    END
  WHEN LOWER('{{ source_table }}') LIKE '%payments%' THEN 
    CASE
        WHEN LOWER('{{ status_name }}') LIKE '%scheduled%' THEN 'Scheduled'
        WHEN LOWER('{{ status_name }}') LIKE '%prepayment%' THEN 'Prepayment'
        ELSE 'Other Loan Status'
    END
  ELSE 'Other'
END
{%- endmacro -%}
