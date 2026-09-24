# data-engineering-assessment-storemesh

## How to Run the Pipeline

0. Clone the repository to your local machine.
1. Install the required dependencies using pip (I recommend using a virtual environment):
    ```bash
    pip install -r requirements.txt
    ```
2. Boot up a local Prefect server (if you don't have one running already):
    ```bash
    prefect server start
    ```
2.1. Copy the API URL from the Prefect server logs (e.g., `http://127.0.0.1:4200/api`)
3. Set the Prefect API URL environment variable in your other terminal session:
    ```bash
    prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
    ```
4. Run the pipeline
    ```bash
    py pipeline.py
    ```
5. Check the Prefect UI to monitor the pipeline's progress and view logs.
6. After the pipeline has completed, you can query the `analytics.db` SQLite database to view the transformed data.
7. To run the unit tests, execute the following command:
    ```bash
    pytest test_pipeline.py
    ```

## Data Exploration

after exploring the data, I found that the following issues exist in the data:
- vw_raw_customers and vw_raw_orders contains null values in some columns
- vw_raw_orders contains customer_id values that do not exist in vw_raw_customers
- vw_raw_orders contains negative total_amount values