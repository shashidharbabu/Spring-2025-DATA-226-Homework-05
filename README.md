# Shashidhar Babu P V D - Spring-2025-DATA-226-Homework-05


## Fetch Stock Price DAG Implementation

This homework is an end-to-end Airflow data pipeline that fetches daily stock price data for Microsoft (MSFT) and Juniper Networks (JNPR) using the Alpha Vantage API, processes the data with Pandas, and loads it into a Snowflake database. The DAG is designed to run daily, and it demonstrates how to build a modular, idempotent pipeline while securely handling sensitive credentials.

## Overview

The DAG is composed of three main tasks:

1. **fetch_stock_data**  
   This task retrieves stock data from the Alpha Vantage API. It:
   - Uses the API key stored securely in Airflow Variables (under the key `VANTAGE_API_KEY`).
   - Fetches daily data for each specified stock symbol.
   - Processes and filters the data to include only the records from the last 90 days.
   - Converts the data into a dictionary format suitable for loading into a database.

2. **create_snowflake_table**  
   This task ensures that the destination table in Snowflake is created (or replaced) before data is loaded. It:
   - Connects to Snowflake using the Airflow connection (`snowflake_default`).
   - Sets the database and schema context.
   - Creates a table named `stock_prices` with the necessary columns and a composite primary key (date and symbol).

3. **load_table**  
   This task loads the processed stock data into the Snowflake table. It:
   - Converts the data dictionary back into a Pandas DataFrame.
   - Iterates over each record and checks if it already exists in the table (ensuring idempotency).
   - Inserts new records while wrapping the process in a SQL transaction (using `BEGIN` and `COMMIT`) to guarantee data consistency.
   - Rolls back the transaction in case of any errors.

The complete DAG implementation can be found in the file [Fetch_stock_price.py]citeturn1file0.

## Prerequisites

- **Apache Airflow**  
  Ensure you have Apache Airflow 2.x (or higher) installed and properly configured.

- **Python Dependencies**  
  The DAG relies on the following Python packages:
  - `requests`
  - `pandas`
  - `pendulum`
  - `snowflake-connector-python`
  - Airflow’s built-in providers (e.g., `airflow.providers.snowflake`)

  Install these dependencies using pip:
  ```bash
  pip install requests pandas pendulum snowflake-connector-python apache-airflow
  ```

- **Airflow Variables and Connections**  
  - **Airflow Variable:** Create a variable named `VANTAGE_API_KEY` (Admin → Variables) and set it to your Alpha Vantage API key.
  - **Airflow Connection:** Configure a Snowflake connection with the ID `snowflake_default` (Admin → Connections). Provide your Snowflake credentials, including username, password, account, warehouse, and other relevant details.

## Setup and Deployment

1. **Clone the Repository**  
   Clone this repository to your local machine and navigate to its directory:
   ```bash
   git clone <your-repo-url>
   cd <repo-directory>
   ```

2. **Deploy the DAG**  
   Place the `Fetch_stock_price.py` file in your Airflow DAGs folder so that Airflow can detect and schedule it.

3. **Configure Airflow Variables and Connections**  
   - Add your Alpha Vantage API key in the Airflow UI under Admin → Variables.
   - Set up your Snowflake connection details in Airflow under Admin → Connections.

4. **Run the DAG**  
   - Trigger the DAG manually from the Airflow UI or wait for its daily schedule.
   - Monitor the progress via the Airflow logs to ensure that data is correctly fetched and loaded into Snowflake.

## How It Works

- **Modularity & Task Dependencies:**  
  The DAG leverages the `@task` decorator to break the pipeline into clear, modular components. This not only improves code readability but also makes testing and maintenance easier.

- **Data Handling and Idempotency:**  
  The data extraction task uses the Alpha Vantage API to get the latest stock data. Before inserting data into Snowflake, the pipeline checks for existing records to avoid duplicates, ensuring that the pipeline remains idempotent when run multiple times.

- **Secure Credential Management:**  
  Sensitive credentials such as the API key and Snowflake connection details are not hardcoded. Instead, they are stored securely in Airflow Variables and Connections, which enhances security and ease of configuration.

- **Robust Database Operations:**  
  By using SQL transactions (with BEGIN/COMMIT/ROLLBACK), the DAG ensures that data integrity is maintained during the loading process. Any errors during the load will trigger a rollback, preventing partial or corrupt data loads.


## Conclusion

This DAG exemplifies how to build a robust, secure, and modular data pipeline using Apache Airflow. By combining API data extraction, data processing with Pandas, and transactional database loading into Snowflake, it provides a practical solution for daily data ingestion and transformation.

Happy pipelining! :)


## Author

- Shashidhar Babu P V D
- shashidharbabu.pasupuletivenkatadurga@sjsu.edu

