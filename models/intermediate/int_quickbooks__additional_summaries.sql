with general_ledger_balances as (
    select *
    from {{ref('int_quickbooks__general_ledger_balances')}}
),

gross_profit as (
    select
        cast('99991' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('5399' as {{ dbt.type_string() }}) as account_number,
        cast('GROSS PROFIT' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('GROSS PROFIT' as {{ dbt.type_string() }}) as account_type,
        cast('GROSS PROFIT' as {{ dbt.type_string() }})as account_sub_type,
        cast('GROSS PROFIT' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(case when account_type = 'Cost of Goods Sold' then period_net_change*-1 else period_net_change end) as period_net_change
    from general_ledger_balances
    where account_type in ('Cost of Goods Sold','Income')
    group by period_first_day, source_relation
),

net_operating_income as (
    select
        cast('99992' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('6899' as {{ dbt.type_string() }}) as account_number,
        cast('NET OPERATING INCOME/EBIDTA' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('NET OPERATING INCOME/EBIDTA' as {{ dbt.type_string() }}) as account_type,
        cast('NET OPERATING INCOME/EBIDTA' as {{ dbt.type_string() }})as account_sub_type,
        cast('NET OPERATING INCOME/EBIDTA' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(case 
                when account_type = 'Expense' then period_net_change*-1
                when account_type = 'Cost of Goods Sold' then period_net_change*-1  
                else period_net_change
            end) as period_net_change
    from general_ledger_balances
    where account_type in ('Cost of Goods Sold','Income','Expense')
    group by period_first_day, source_relation
),

net_other_income as (
    select
        cast('99993' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('9998' as {{ dbt.type_string() }}) as account_number,
        cast('NET OTHER INCOME' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('NET OTHER INCOME' as {{ dbt.type_string() }}) as account_type,
        cast('NET OTHER INCOME' as {{ dbt.type_string() }})as account_sub_type,
        cast('NET OTHER INCOME' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(case 
                when account_type = 'Other Expense' then period_net_change*-1
                else period_net_change
            end) as period_net_change
    from general_ledger_balances
    where account_type in ('Other Income','Other Expense')
    group by period_first_day, source_relation
),

net_income as (
    select
        cast('99994' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('9999' as {{ dbt.type_string() }}) as account_number,
        cast('NET INCOME' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('NET INCOME' as {{ dbt.type_string() }}) as account_type,
        cast('NET INCOME' as {{ dbt.type_string() }})as account_sub_type,
        cast('NET INCOME' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(case 
                when account_type = 'Expense' then period_net_change*-1
                when account_type = 'Cost of Goods Sold' then period_net_change*-1  
                when account_type = 'Other Expense' then period_net_change*-1
                else period_net_change
            end) as period_net_change
    from general_ledger_balances
    where account_type in ('Cost of Goods Sold','Income','Expense','Other Income','Other Expense')
    group by period_first_day, source_relation
),

total_revenue as (
    select
        cast('99995' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('4899' as {{ dbt.type_string() }}) as account_number,
        cast('TOTAL REVENUE' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('TOTAL REVENUE' as {{ dbt.type_string() }}) as account_type,
        cast('TOTAL REVENUE' as {{ dbt.type_string() }})as account_sub_type,
        cast('Revenue' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(period_net_change) as period_net_change
    from general_ledger_balances
    where account_type in ('Income')
    group by period_first_day, source_relation
),

total_cost_of_goods_sold as (
    select
        cast('99996' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('5398' as {{ dbt.type_string() }}) as account_number,
        cast('TOTAL COST OF GOODS SOLD' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('TOTAL COST OF GOODS SOLD' as {{ dbt.type_string() }}) as account_type,
        cast('TOTAL COST OF GOODS SOLD' as {{ dbt.type_string() }})as account_sub_type,
        cast('Expense' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(period_net_change*-1) as period_net_change
    from general_ledger_balances
    where account_type in ('Cost of Goods Sold')
    group by period_first_day, source_relation
),

total_expenses as (
    select
        cast('99997' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('6898' as {{ dbt.type_string() }}) as account_number,
        cast('TOTAL EXPENSES' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('TOTAL EXPENSES' as {{ dbt.type_string() }}) as account_type,
        cast('TOTAL EXPENSES' as {{ dbt.type_string() }})as account_sub_type,
        cast('Expense' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(period_net_change*-1) as period_net_change
    from general_ledger_balances
    where account_type in ('Expense')
    group by period_first_day, source_relation
),

total_other_income as (
    select
        cast('99998' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('4999' as {{ dbt.type_string() }}) as account_number,
        cast('TOTAL OTHER INCOME' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('TOTAL OTHER INCOME' as {{ dbt.type_string() }}) as account_type,
        cast('TOTAL OTHER INCOME' as {{ dbt.type_string() }})as account_sub_type,
        cast('Revenue' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(period_net_change) as period_net_change
    from general_ledger_balances
    where account_type in ('Other Income')
    group by period_first_day, source_relation
),

total_other_expenses as (
    select
        cast('99999' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('9997' as {{ dbt.type_string() }}) as account_number,
        cast('TOTAL OTHER EXPENSES' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('TOTAL OTHER EXPENSES' as {{ dbt.type_string() }}) as account_type,
        cast('TOTAL OTHER EXPENSES' as {{ dbt.type_string() }})as account_sub_type,
        cast('Expense' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('income_statement' as {{ dbt.type_string() }}) as financial_statement_helper,
        cast({{ dbt.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt.last_day("period_first_day", "month") }} as period_last_day,
        sum(period_net_change*-1) as period_net_change
    from general_ledger_balances
    where account_type in ('Other Expense')
    group by period_first_day, source_relation
),

additional_summaries as (
    select * from gross_profit
    union
    select * from net_operating_income
    union
    select * from net_other_income
    union
    select * from net_income    
    union
    select * from total_revenue
    union
    select * from total_cost_of_goods_sold
    union
    select * from total_expenses
    union
    select * from total_other_income
    union
    select * from total_other_expenses
),

additional_summaries_ending as (
    select
        *,
        sum(coalesce(period_net_change,0)) over (order by account_name, period_first_day, period_first_day rows unbounded preceding) as period_ending_balance
    from additional_summaries
),

final as (
    select
        account_id,
        source_relation,
        account_number,
        account_name,
        is_sub_account,
        parent_account_number,
        parent_account_name,
        account_type,
        account_sub_type,
        account_class,
        class_id,
        financial_statement_helper,
        date_year,
        period_first_day,
        period_last_day,
        period_net_change,
        lag(coalesce(period_ending_balance,0)) over (order by account_name, period_first_day) as period_beginning_balance,
        period_ending_balance
    from additional_summaries_ending
)

select *
from final
