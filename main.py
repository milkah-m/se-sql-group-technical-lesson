import sqlite3
import pandas as pd
conn = sqlite3.Connection('data.sqlite')

#1. Count of Customers by Country

countries = pd.read_sql("""
SELECT country, COUNT(*) as num_customers
FROM customers
GROUP BY country
""", conn  
)
print(countries)

countries = pd.read_sql("""
SELECT country, COUNT(*) as num_customers
FROM customers
GROUP BY 1
""", conn  
).head(10)
print(countries)

#2. calculate various summary statistics about payments, grouped by customer
customer_stats = pd.read_sql("""
SELECT customerNumber,
COUNT(*) as num_purchases,
MIN(CAST (amount as INTEGER)) as min_purchase,
MAX(CAST (amount as INTEGER)) as max_purchase,
AVG(CAST (amount as INTEGER)) as avg_purchase,
SUM(CAST (amount as INTEGER)) as total_spent
FROM payments
GROUP BY customerNumber
""", conn  
)
print(customer_stats)

year_specific = pd.read_sql("""
SELECT customerNumber,
COUNT(*) as num_purchases,
MIN(CAST (amount as INTEGER)) as min_purchase,
MAX(CAST (amount as INTEGER)) as max_purchase,
AVG(CAST (amount as INTEGER)) as avg_purchase,
SUM(CAST (amount as INTEGER)) as total_spent
FROM payments
WHERE strftime("%Y", paymentDate) = "2004"
GROUP BY customerNumber
""", conn  
)
print(year_specific)

#3. filter to only select aggregated payment information about customers with average payment amounts over 50,000

bowlers = pd.read_sql("""
SELECT customerNumber,
COUNT(*) as num_purchases,
MIN(CAST (amount as INTEGER)) as min_purchase,
MAX(CAST (amount as INTEGER)) as max_purchase,
AVG(CAST (amount as INTEGER)) as avg_purchase,
SUM(CAST (amount as INTEGER)) as total_spent
FROM payments
GROUP BY customerNumber
HAVING avg_purchase > 50000
""", conn  
)
print(bowlers)


#4. filter based on customers who have made at least 2 purchases of over 50000 each
super_bowlers = pd.read_sql("""
SELECT customerNumber, 
COUNT(*) as num_purchases,
MIN(CAST (amount as INTEGER)) as min_purchase,
MAX(CAST (amount as INTEGER)) as max_purchase,
AVG(CAST (amount as INTEGER)) as avg_purchase,
SUM(CAST (amount as INTEGER)) as total_spent,
CAST (amount as INTEGER) as int_amount
FROM payments
WHERE int_amount > 50000
GROUP BY customerNumber
HAVING num_purchases >= 2
""", conn  
)
print(super_bowlers)

#5.  find the customer with the lowest total amount spent, who nevertheless fits the criteria described above.
mini_bowler = pd.read_sql("""
SELECT customerNumber, 
COUNT(*) as num_purchases,
MIN(CAST (amount as INTEGER)) as min_purchase,
MAX(CAST (amount as INTEGER)) as max_purchase,
AVG(CAST (amount as INTEGER)) as avg_purchase,
SUM(CAST (amount as INTEGER)) as total_spent,
CAST (amount as INTEGER) as int_amount
FROM payments
WHERE int_amount > 50000
GROUP BY customerNumber
HAVING num_purchases >= 2
ORDER BY total_spent 
LIMIT 1
""", conn  
)
print(mini_bowler)

conn.close()