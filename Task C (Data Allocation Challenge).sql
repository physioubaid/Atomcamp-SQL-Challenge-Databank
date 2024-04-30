#Task C: Data Allocation Challenge
#We need to generate required data elements mentioned in the challenge before proceeding with the request.
#The Allocation scenarios will bae upon these required data elements.
-----
#1: running customer balance column that impacts each transaction
# Calculate running balance for each customer based upon their transaction types
# txn_amount is adjusted to show negative balance for txn types 'purchase' and 'withdrawal.
#Sol:
Select customer_id,
       txn_date,
       txn_amount,
       txn_type,
       SUM(case when txn_type = 'deposit' then txn_amount
		when txn_type = 'withdrawal' then -txn_amount
		when txn_type = 'purchase' then -txn_amount
		else 0
	   end) over(partition by customer_id order by txn_date) as running_balance
from customer_transactions;

#2: Customer balance at the end of each month
# Calculate ending balance for each customer for each month
# txn_amount is adjusted to show negative balance for txn types 'purchase' and 'withdrawal.
#Sol:
Select customer_id,
	month(txn_date) as month,
	monthname(txn_date) as month_name,
	SUM(case when txn_type = 'deposit' then txn_amount
			 when txn_type = 'withdrawal' then -txn_amount
			 when txn_type = 'purchase' then -txn_amount
	   else 0
	   end) as closing_balance
from customer_transactions
group by customer_id, month(txn_date), monthname(txn_date);

#3: minimum, average, and maximum values of the running balance for each customer
#Create a CTE 'running_balance' to find the running balance of each customer based on the order of transactions.
#Calculate the minimum, maximum, and average balance for each customer.
#Sol:
With running_balance as
(
	select customer_id,
	       txn_date,
	       txn_type,
	       txn_amount,
	       SUM(case when txn_type = 'deposit' then txn_amount
			when txn_type = 'withdrawal' then -txn_amount
			when txn_type = 'purchase' then -txn_amount
			else 0
		   end) over(partition by customer_id order by txn_date) as running_balance
	from customer_transactions
)

select customer_id,
       avg(running_balance) as avg_running_balance,
       min(running_balance) as min_running_balance,
       max(running_balance) as max_running_balance
from running_balance
group by customer_id;

#Now, With these data elements, let's run the scenarios in the given options

#data is allocated based off the amount of money at the end of the previous month.
#How much data can be required on monthly basis in this scenario?
#Sol: 
#--use a CTE to calculate net txn amount for each customer for each txn
#-- another CTE to calculate running balance for each, utilizing preceding as well as current rows,
#-- additional CTE to calcualte ending bal for each customer
#--final query to calculate the required data needs for allocation to each month

With txn_amt_cte as
(
	select customer_id,
		   txn_date,
		   month(txn_date) as txn_month,
		   txn_type,
		   case when txn_type = 'deposit' then txn_amount 
				else -txn_amount 
		   end as net_transaction_amt
	from customer_transactions
),
running_balance_cte as
(
	select customer_id,
		   txn_date,
		   txn_month,
		   net_transaction_amt,
		   sum(net_transaction_amt) over(partition by customer_id, txn_month order by txn_date
		   rows between unbounded preceding and current row) as running_balance
	from txn_amt_cte
),
end_month_bal_cte as
(
	select customer_id,
		   txn_month,
		   max(running_balance) as month_end_balance
	from running_balance_cte
	group by customer_id, txn_month
)

select txn_month,
	   sum(month_end_balance) as data_required_per_month
from end_month_bal_cte
group by txn_month
order by data_required_per_month desc;

#January required most data allocation (368010) of the list whereas April requires the least (53434)
#Data Allocation varies across all month, this can be due to factors like running balance & end month balance for customers
#In January & March data required was more than February and April, this indicates the likeliness of customers to have higher balances during the former months.
#The number of days in January & March can also be a factor in higher data requirement & end month balances

#Option-2: data is allocated on the average amount of money kept in the account in the previous 30 days
#How much data would have been required on a monthly  basis?
#Sol:
#--use a CTE to calculate net txn amount for each customer for each txn
#-- another CTE to calculate running bal for each customer for each txn based on net txn amount
#-- additional CTE to calcualte avg running balance for each customer for all time
#--final query to calculate the required data needs for allocation to each month utilizing the above ctes and using funcitons like groupby and sum.

with txn_amt_cte as
	(select customer_id,
		month(txn_date) as txn_month,
        sum(case when txn_type ='deposit' then txn_amount
							else - txn_amount
                            End) as net_txn_amt
	from customer_transactions
    group by customer_id, month(txn_date)
    order by customer_id
    ),

running_bal_cte as 
(
	select customer_id,
			txn_month,
            net_txn_amt,
            sum(net_txn_amt) over(partition by customer_id order by txn_month) as running_bal
	from txn_amt_cte
),

	avg_bal_cte as
(
	select customer_id,
			avg(running_bal) as avg_running_bal
	from running_bal_cte
    group by customer_id
)
select txn_month,
			round(sum(avg_running_bal), 0) as data_req_per_month
from running_bal_cte r
join avg_bal_cte a
on r.customer_id = a.customer_id
group by txn_month
order by data_req_per_month;

# Significant Insights;
# Based on output, all four months average running balance is negative, indicating customers withdraw more balance than deposits.
# Feb and March have more data required, as compared to Jan and April.

#Option3: data is updated real-time.
#How much data would have been required on a monthly basis?
#Sol:
#--use a CTE to calculate net txn amount for each customer for each txn
#-- another CTE to calculate running bal for each customer for each txn based on sum of net txn amount
#--final query to calculate the required data needs for for allocation.

with txn_amt_cte as 
( 
    select 
        customer_id,
        txn_date,
        month(txn_date) as txn_month,
        txn_type,
        txn_amount,
        case 
            when txn_type = 'deposit' then txn_amount 
            else -txn_amount 
        end as net_txn_amt
    from 
        customer_transactions
),
running_bal_cte as
(
    select 
        customer_id,
        txn_month,
        sum(net_txn_amt) over (partition by customer_id order by txn_month) as running_balance
    from 
        txn_amt_cte
)
select 
    txn_month,
    sum(running_balance) as data_req_per_month
from 
    running_bal_cte
group by 
    txn_month
order by 
    data_req_per_month;
    
#January has a positive number for data required, indicating higher balances.
#March has the highest amount of data required, indicating higher level of customer activity in this month, thus requiring higher number of data.

#___________________THE END________________________________#

