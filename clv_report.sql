SELECT 
	customer_id,
	name as full_name,
	count(*) as total_orders_placed,
	sum(total_amount) as lifetime_value_usd,
	signup_date as customer_cohort
FROM fct_orders
JOIN dim_customers USING (customer_id)
GROUP BY customer_id
ORDER BY sum(total_amount) DESC