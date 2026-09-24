import sqlite3
from prefect import flow, task, get_run_logger
import pandas as pd

def get_connection(database_path="shopdata.db"):
    return sqlite3.connect(database_path)


@task(
    name="Extract Data",
    description="Extract source data from database views",
    retries=3,
    retry_delay_seconds=5,
)
def extract_data():
    logger = get_run_logger()
    logger.info("Extracting data from database views...")

    conn = get_connection()

    try:
        exchange_rates = pd.read_sql_query(
            "SELECT * FROM vw_exchange_rates",
            conn
        )

        customers = pd.read_sql_query(
            "SELECT * FROM vw_raw_customers",
            conn
        )

        orders = pd.read_sql_query(
            "SELECT * FROM vw_raw_orders",
            conn
        )

        logger.info("Data extraction complete.")

        return exchange_rates, customers, orders

    except Exception:
        logger.exception("Data extraction failed.")
        raise

    finally:
        conn.close()


def transform_customers(customers):
    customers = customers.copy()

    customers["signup_date"] = pd.to_datetime(
        customers["signup_date"],
        errors="coerce"
    )

    customers = (
        customers
        .sort_values("signup_date")
        .drop_duplicates(
            subset=["customer_id"],
            keep="last"
        )
    )

    # since the rules for NA values for phone are not specified, I will fill them with empty string
    customers["phone"] = (
        customers["phone"]
        .astype("string")
        .str.strip()
        .str.replace(r"[^0-9]", "", regex=True)
        .fillna("")
    )

    customers["email"] = (
        customers["email"]
        .astype("string")
        .str.strip()
        .fillna("unknown@domain.com")
    )

    return customers

def transform_orders(orders, exchange_rates):
    orders = orders.copy()
    exchange_rates = exchange_rates.copy()

    orders["total_amount"] = pd.to_numeric(
        orders["total_amount"],
        errors="coerce"
    )

    orders["order_date"] = pd.to_datetime(
        orders["order_date"],
        errors="coerce"
    ).dt.date

    exchange_rates["date"] = pd.to_datetime(
        exchange_rates["date"],
        errors="coerce"
    ).dt.date

    # Remove invalid amounts
    orders = orders[
        orders["total_amount"] > 0
    ]

    # Join exchange rate onto each order
    orders = orders.merge(
        exchange_rates,
        left_on=["currency", "order_date"],
        right_on=["currency", "date"],
        how="left",
        validate="many_to_one"
    )

    # Detect missing rates
    missing_rates = orders["rate_to_usd"].isna()

    # Convert amount if rate is available, otherwise do not convert (keep original amount)
    orders.loc[~missing_rates, "total_amount"] = (
        orders.loc[~missing_rates, "total_amount"] *
        orders.loc[~missing_rates, "rate_to_usd"]
    )

    orders["currency"] = "USD"

    return orders

@task(
    name="Transform Data",
    description="Clean customers and standardize order amounts to USD",
)
def transform_data(exchange_rates, customers, orders):

    logger = get_run_logger()
    logger.info("Transforming data...")

    # ------------------
    # Customers
    # ------------------

    logger.info("Transforming customers data...")

    customers = transform_customers(customers)

    # ------------------
    # Orders
    # ------------------

    logger.info("Transforming orders data...")

    orders = transform_orders(orders, exchange_rates)

    # Format dates as strings for SQLite compatibility
    customers["signup_date"] = (
        customers["signup_date"]
        .dt.strftime("%Y-%m-%d")
    )

    orders["order_date"] = (
        pd.to_datetime(orders["order_date"])
        .dt.strftime("%Y-%m-%d")
    )

    logger.info(
        "Transformation complete: %d customers, %d orders.",
        len(customers),
        len(orders)
    )

    return customers, orders[["order_id", "customer_id", "total_amount", "currency", "status", "order_date"]]


@task(
    name="Load Data",
    description="Load transformed data into analytics tables",
    retries=3,
    retry_delay_seconds=5,
)
def load_data(customers, orders):
    conn = get_connection("analytics.db")
    logger = get_run_logger()

    try:
        cursor = conn.cursor()
        logger.info("Loading data into analytics tables...")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                phone TEXT,
                signup_date TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fct_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                total_amount REAL,
                currency TEXT,
                status TEXT,
                order_date TEXT,
                FOREIGN KEY (customer_id)
                    REFERENCES dim_customers(customer_id)
            )
        """)

        cursor.executemany("""
            INSERT INTO dim_customers (
                customer_id,
                name,
                email,
                phone,
                signup_date
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(customer_id) DO UPDATE SET
                name = excluded.name,
                email = excluded.email,
                phone = excluded.phone,
                signup_date = excluded.signup_date
        """, customers.itertuples(index=False, name=None))

        cursor.executemany("""
            INSERT INTO fct_orders (
                order_id,
                customer_id,
                total_amount,
                currency,
                status,
                order_date
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_id = excluded.customer_id,
                total_amount = excluded.total_amount,
                currency = excluded.currency,
                status = excluded.status,
                order_date = excluded.order_date
        """, orders.itertuples(index=False, name=None))

        conn.commit()
        logger.info("Data loading complete.")
    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


@flow(name="ETL Pipeline", description="Extract, Transform, and Load data from views to new tables")
def etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting ETL pipeline...")
    exchange_rates, customers, orders = extract_data()
    transformed_customers, transformed_orders = transform_data(exchange_rates, customers, orders)
    load_data(transformed_customers, transformed_orders)


if __name__ == "__main__":
    etl_pipeline()