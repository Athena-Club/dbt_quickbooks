/*
Table that creates a debit record to accounts payable and a credit record to the specified cash account.
*/

--To disable this model, set the using_bill_payment variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_bill', True)) }}

with bill_payments as (

    select *
    from {{ ref('stg_quickbooks__bill_payment') }}
),

bill_payment_lines as (

    select *
    from {{ ref('stg_quickbooks__bill_payment_line') }}
),

bills as (
    select *
    from {{ ref('stg_quickbooks__bill') }}
),

bill_linked_payments as (
    select *
    from {{ ref('stg_quickbooks__bill_linked_txn') }}
),

bill_pay_currency as (
    select
        bill_linked_payments.bill_payment_id,
        sum(bills.total_amount*coalesce(exchange_rate,1)) as total_amount,
        bill_linked_payments.source_relation
    from bill_linked_payments 
    left join bills
        on bill_linked_payments.bill_id = bills.bill_id
        and bill_linked_payments.source_relation = bills.source_relation
    where bill_linked_payments.bill_payment_id is not null
        and bills.currency_id != case when bills.source_relation = 'quickbooks' then 'USD' else 'CAD' end
    group by 1,3
),
accounts as (

    select *
    from {{ ref('stg_quickbooks__account') }}
),

ap_accounts as (

    select
        account_id,
        currency_id,
        source_relation
    from accounts

    where account_type = 'Accounts Payable'
        and is_active
        and not is_sub_account
),

exchange_gl_accounts as (
    select
        account_id,
        source_relation
    from accounts
    where name = 'Exchange Gain or Loss'
        and is_active
        and not is_sub_account        
),
bill_payment_join as (
    select
        bill_payments.bill_payment_id as transaction_id,
        bill_payments.source_relation,
        row_number() over(partition by bill_payments.bill_payment_id order by bill_payments.transaction_date) - 1 as index,
        bill_payments.transaction_date,
        round(coalesce(bill_pay_currency.total_amount,bill_payments.total_amount*coalesce(bill_payments.exchange_rate,1)),2) as payment_amount,
        round(bill_payments.total_amount*coalesce(bill_payments.exchange_rate,1),2) as bank_amount,
        coalesce(bill_payments.credit_card_account_id,bill_payments.check_bank_account_id) as payment_account_id,
        ap_accounts.account_id,
        bill_payments.vendor_id,
        bill_payments.department_id
    from bill_payments
    left join bill_pay_currency
       on bill_payments.bill_payment_id = bill_pay_currency.bill_payment_id
       and bill_payments.source_relation = bill_pay_currency.source_relation
    left join ap_accounts
        on bill_payments.currency_id = ap_accounts.currency_id
        and bill_payments.source_relation = ap_accounts.source_relation
),

final as (

    select
        transaction_id,
        source_relation,
        index,
        transaction_date,
        cast(null as {{ dbt.type_string() }}) as customer_id,
        vendor_id,
        bank_amount as amount,
        payment_account_id as account_id,
        cast(null as {{ dbt.type_string() }}) as class_id,
        department_id,
        'credit' as transaction_type,
        'bill payment' as transaction_source
    from bill_payment_join

    union all

    select
        transaction_id,
        source_relation,
        index,
        transaction_date,
        cast(null as {{ dbt.type_string() }}) as customer_id,
        vendor_id,
        payment_amount as amount,
        account_id,
        cast(null as {{ dbt.type_string() }}) as class_id,
        department_id,
        'debit' as transaction_type,
        'bill payment' as transaction_source
    from bill_payment_join
    union all

    select
        transaction_id,
        bill_payment_join.source_relation,
        index,
        transaction_date,
        cast(null as {{ dbt.type_string() }}) as customer_id,
        vendor_id,
        coalesce(bank_amount-payment_amount,0) as amount,
        exchange_gl_accounts.account_id as account_id,
        cast(null as {{ dbt.type_string() }}) as class_id,
        department_id,
        'debit' as transaction_type,
        'bill payment' as transaction_source
    from bill_payment_join
    left join exchange_gl_accounts
        on bill_payment_join.source_relation = exchange_gl_accounts.source_relation
    where payment_amount != bank_amount
)

select *
from final
