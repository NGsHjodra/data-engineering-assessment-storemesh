-- Since I have only read-only access to these views, I will start with a quick skim of the data.
SELECT * FROM vw_exchange_rates LIMIT 20
SELECT * FROM vw_raw_customers LIMIT 20
SELECT * FROM vw_raw_orders LIMIT 20


-- Null Check
SELECT
    count(*) AS total_rows,
    sum(currency IS NULL) AS null_currency,
    sum(rate_to_usd IS NULL) AS null_rate,
    sum(date IS NULL) AS null_date
FROM vw_exchange_rates;

SELECT
    count(*) AS total_rows,
    sum(customer_id IS NULL) AS null_customer_id,
    sum(full_name IS NULL) AS null_full_name,
    sum(email IS NULL) AS null_email,
    sum(phone IS NULL) AS null_phone,
    sum(signup_date IS NULL) AS null_signup_date
FROM vw_raw_customers;

SELECT
    count(*) AS total_rows,
    sum(order_id IS NULL) AS null_order_id,
    sum(customer_id IS NULL) AS null_customer_id,
    sum(order_date IS NULL) AS null_order_date,
    sum(total_amount IS NULL) AS null_total_amount,
    sum(currency IS NULL) AS null_currency,
    sum(status IS NULL) AS null_status
FROM vw_raw_orders;

-- vw_raw_customers and vw_raw_orders contains null


-- Duplicate Check
SELECT
	currency,
    count(*) AS total_rows
FROM vw_exchange_rates
GROUP BY currency
HAVING count(*) > 1;

SELECT
    customer_id,
	count(*) AS total_rows
FROM vw_raw_customers
GROUP BY customer_id
HAVING count(*) > 1;

SELECT
    order_id,
	count(*) AS total_rows
FROM vw_raw_orders
GROUP BY order_id
HAVING count(*) > 1;

-- vw_raw_customers contains duplicates customer_id


-- Range Check
SELECT
    min(total_amount) AS min_amount,
	max(total_amount) AS max_amount
FROM vw_raw_orders;

-- vw_raw_orders contains negative total_amount