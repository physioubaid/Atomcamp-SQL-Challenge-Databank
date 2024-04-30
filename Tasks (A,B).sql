set sql_safe_updates = 0;

#Task A. Customer Nodes Exploration. 
#1: How many unique nodes are there on the Data Bank system?
#used COUNT DISTINCT to find unique nodes in dataset
SELECT COUNT(DISTINCT node_id) AS unique_nodes 
FROM data_bank.customer_nodes;
#There are 5 unique nodes

#2:What is the number of nodes per region? 
#count function to find number of nodes per region
SELECT c.region_id, region_name, COUNT(node_id) AS nodes_per_region
FROM customer_nodes c INNER JOIN regions r ON c.region_id = r.region_id
GROUP BY c.region_id, region_name;
# Names of region from highest to lowest number of nodes are: Australia,America,Africa,Asia,Europe.

#3: How many customers are allocated to each region?
#use COUNT DISTINCT function to find number of customers per region
SELECT region_id, COUNT(DISTINCT customer_id) AS customers_per_region
FROM data_bank.customer_nodes
GROUP BY region_id;
#Order by highest to lowest number of customers, region 1,2,3,4,5.

#4: How many days on average are customers reallocated to a different node?
SELECT AVG(duration) AS avg_days_reallocation
FROM (
    SELECT customer_id, DATEDIFF(end_date, start_date) AS duration
    FROM data_bank.customer_nodes
    WHERE end_date != '99991231' -- Exclude rows where end_date is NULL
) AS durations
WHERE duration <= 1000; -- Exclude durations that are unrealistically large i.e. outliers / anomalies
#Average Duration is approximately 14 days.

#5:What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
#Reference taken from GitHub Repository of Sajjad Haider   
WITH RankedNodes AS (
	Select
			r.region_id as rn_id,
            r.region_name as rn_name,
            DATEDIFF(end_date, start_date) AS days_spent,
            ((ROW_NUMBER()
				OVER (PARTITION by r.region_name
			order by DATEDIFF(c.end_date, c.start_date))-1)/count(*)
            over (partition by r.region_name))*100 as percentile
	From customer_nodes c join regions r
    ON r.region_id = c.region_id
    where end_date != '99991231'
    order by r.region_name, percentile)
Select
		rn_name as region,
	MIN(CASE WHEN percentile >= 50 THEN days_spent END) as median,
    MIN(CASE WHEN percentile >= 80 THEN days_spent END) as percentile_80,
    MIN(CASE WHEN percentile >= 95 THEN days_spent END) as percentile_95
from RankedNodes
group by rn_name;  
#All regions have median = 15, percentile_95 = 28, whereas Africa & Europe has percentile_80=24, rest have 23. 

#Task B. Customer Transactions
#1: What is the unique count and total amount for each transaction type?
# use COUNT DISTINCT to find unique count of each txn type, SUM to calculate total amount of all txn types.
SELECT 
    txn_type,
    COUNT(DISTINCT customer_id) AS unique_count,
    SUM(txn_amount) AS total_amount
FROM 
    customer_transactions
GROUP BY 
    txn_type;
#Most txn types are deposits, followed by purchase then withdrawal.
    
#2:What is the average total historical deposit counts and amounts for all customers?
#Create An outer query calculating the avg of total deposit counts and total deposit amounts
#A subquery with COUNT function to count all txn with 'deposit' type for total_deposit_counts
# SUM Function to add all the deposit amoount as total_deposit_amounts
#The query returns the data as "customer_summary" CTE
SELECT 
    AVG(total_deposit_counts) AS avg_deposit_counts,
    AVG(total_deposit_amount) AS avg_deposit_amount
FROM (
    SELECT 
        customer_id,
        COUNT(CASE WHEN txn_type = 'deposit' THEN 1 END) AS total_deposit_counts,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount END) AS total_deposit_amount
    FROM 
        customer_transactions
    GROUP BY 
        customer_id
) AS customer_summary;
#Average deposit count is 5 and avg deposit amount is approximately 2718

#3: For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
#The query calculates the number of unique customers who with atleast on txn in each month (Using COUNT(DISTINCT customer_id) and conditional COUNT with CASE statements)
#It then groups data by year and month based on transaction dates and only includes customers with relevant transaction types ('deposit', 'purchase', 'withdrawal'). 
#(Using GROUP BY with YEAR(txn_date) and MONTH(txn_date), and filtering with WHERE clause)
SELECT 
    YEAR(txn_date) AS year,
    MONTH(txn_date) AS month,
    COUNT(DISTINCT customer_id) AS customers_with_multiple_deposits_and_other_transaction
FROM 
    customer_transactions
WHERE 
    txn_type IN ('deposit', 'purchase', 'withdrawal')
GROUP BY 
    YEAR(txn_date), MONTH(txn_date)
HAVING 
    COUNT(CASE WHEN txn_type = 'deposit' THEN 1 END) > 1
    AND (COUNT(CASE WHEN txn_type IN ('purchase', 'withdrawal') THEN 1 END) >= 1);
#January had the highest amount of transactions(500) followed by March(456),February(455) and April(309) at the last.

#4: What is the closing balance for each customer at the end of the month?
#The query calculates the closing balance for each customer at the end of each month by summing up their transaction amounts using the SUM() function.
#It is grouped by customer ID and month using the GROUP BY function, specifically grouping by customer_id, YEAR(txn_date), and MONTH(txn_date).
SELECT 
    customer_id,
    YEAR(txn_date) AS year,
    MONTH(txn_date) AS month,
    SUM(txn_amount) AS closing_balance
FROM 
    customer_transactions
WHERE 
    txn_date <= LAST_DAY(txn_date)
GROUP BY 
    customer_id, YEAR(txn_date), MONTH(txn_date);
#Long output, customer_id 110 had all time highest closing balance (in January), while id 286 had all time lowest closing balance (in Feb)

#5: What is the percentage of customers who increase their closing balance by more than 5%?
#Create CTE 'ClosingBalances' with customer id and sum of txn amount as closing balance, where txn_dates are less than last date.
#used select & count function to calculate percentages of closing balances
#In another CTE 'balance_close_summary' we calculated change in balance with max-min balance / min balance. 
#Closing argument conditioned the output to show where balance change was greater than 5 percent.
WITH ClosingBalances AS (
    SELECT 
        customer_id,
        YEAR(txn_date) AS year,
        MONTH(txn_date) AS month,
        SUM(txn_amount) AS closing_balance
    FROM 
        customer_transactions
    WHERE 
        txn_date <= LAST_DAY(txn_date)
    GROUP BY 
        customer_id, YEAR(txn_date), MONTH(txn_date)
)
SELECT 
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ClosingBalances) AS percentage_increase
FROM 
    (
        SELECT 
            customer_id,
            ((MAX(closing_balance) - MIN(closing_balance)) / MIN(closing_balance)) AS balance_increase
        FROM 
            ClosingBalances
        GROUP BY 
            customer_id
    ) AS balance_increase_summary
WHERE 
    balance_increase > 0.05;
