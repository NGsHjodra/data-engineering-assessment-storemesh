from pipeline import transform_customers, transform_orders
import pandas as pd

def test_transform_data():
    # Sample data for testing
    exchange_rates = pd.DataFrame({
        'currency': ['USD', 'EUR', 'GBP'],
        'rate_to_usd': [1.0, 1.2, 1.3],
        'date': ['2023-01-01', '2023-01-01', '2023-01-01']
    })

    customers = pd.DataFrame({
        'customer_id': [1, 2],
        'full name': ['Alice', 'Bob'],
        'email': ['alice@example.com', 'bob@example.com'],
        'phone': ['123-456-7890', '987-654-3210'],
        'signup_date': ['2023-01-01', '2023-02-01']
    })

    orders = pd.DataFrame({
        'order_id': [1, 2],
        'customer_id': [1, 2],
        'order_date': ['2023-01-01', '2023-01-01'],
        'total_amount': [100.0, 200.0],
        'currency': ['USD', 'EUR'],
        'status': ['COMPLETED', 'COMPLETED']
    })

    # Call the transform_data function
    transformed_customers = transform_customers(customers)
    transformed_orders = transform_orders(orders, exchange_rates)

    # Add assertions to verify the transformation
    assert len(transformed_customers) == 2
    assert len(transformed_orders) == 2

    assert transformed_customers['phone'].iloc[0] == '1234567890' # Remove non-numeric characters from phone numbers
    assert transformed_orders['total_amount'].iloc[1] == 240.0  # EUR to USD conversion