from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
# from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import requests
import pandas as pd
import pendulum

# Default arguments for the DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='Fetch_stock_price',                               # Unique identifier for the DAG
    schedule='@daily',                                  # DAG will run daily
    start_date=pendulum.today('UTC').add(days=-1),    # Start date is 1 day ago
    catchup=False,                                    # No catchup for missed runs
    default_args=default_args,                        # Set retry and delay settings
    description='Daily stock data pipeline for MSFT and JNPR using Snowflake and Vantage API'
) as dag:

    @task
    def fetch_stock_data():
        symbols = ["MSFT", "JNPR"]
        
        api_key = Variable.get("VANTAGE_API_KEY")  

        dfs = []

        for symbol in symbols:
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact"

            response = requests.get(url)
            data = response.json()

            time_series = data.get('Time Series (Daily)', {})
            df = pd.DataFrame.from_dict(time_series, orient='index')
            df = df.reset_index().rename(columns={
                'index': 'date',
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })

            # Adding the stock symbol to each row
            df['symbol'] = symbol

            # Converting the date column to datetime format
            df['date'] = pd.to_datetime(df['date'])

            # Append the DataFrame to the list
            dfs.append(df)

        # Concatenate all DataFrames into a single DataFrame
        final_df = pd.concat(dfs, ignore_index=True)

        # Ensuring the 'date' column is in datetime format
        final_df['date'] = pd.to_datetime(final_df['date'], errors='coerce')

        # Checking for any conversion issues
        if final_df['date'].isnull().any():
            print("Warning: Some dates could not be converted. Please check your input data.")

        # Filtering the data to include only records from the last 90 days
        ninety_days_ago = datetime.now() - timedelta(days=90)
        final_df = final_df[final_df['date'] >= ninety_days_ago]

        # Ensure there is data for both stocks
        unique_symbols = final_df['symbol'].unique()
        print("Unique symbols in the DataFrame after filtering:", unique_symbols)

        if "AAPL" not in unique_symbols or "GOOGL" not in unique_symbols:
            print("Warning: Missing data for one or both stocks.")

        # Converting 'date' column to string format 'YYYY-MM-DD' for insertion
        final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')

        return final_df.to_dict()

    @task
    def create_snowflake_table():
        # Using Airflow's Snowflake connection
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Using the correct database and schema
            cursor.execute("USE DATABASE stock_price")
            cursor.execute("USE SCHEMA raw_data")

            # Create or replace the table
            create_table_query = """
            CREATE OR REPLACE TABLE stock_prices (
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                symbol VARCHAR,
                PRIMARY KEY (date, symbol)
            );
            """
            cursor.execute(create_table_query)
        finally:
            cursor.close()
            conn.close()

    @task
    def load_table(stock_data):
        # Converting stock_data dictionary back to DataFrame
        df = pd.DataFrame.from_dict(stock_data)

        # Using Airflow's Snowflake connection
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Using the appropriate database and schema
            cursor.execute("USE DATABASE stock_price")
            cursor.execute("USE SCHEMA raw_data")

            # Starting a transaction
            cursor.execute("BEGIN")

            # Inserting only the filtered data (last 90 days) into Snowflake with idempotency check
            for index, row in df.iterrows():
                print(f"Processing symbol: {row['symbol']} for date: {row['date']}")

                # Checking if the record already exists
                check_query = """
                SELECT COUNT(*) FROM stock_prices
                WHERE date = %s AND symbol = %s
                """
                cursor.execute(check_query, (row['date'], row['symbol']))
                exists = cursor.fetchone()[0]  # Get the count

                if exists == 0:
                    print(f"Inserting record for {row['symbol']} on {row['date']}")
                    insert_query = """
                    INSERT INTO stock_prices (date, open, high, low, close, volume, symbol)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        row['date'],  # Date is now a string
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['volume'],
                        row['symbol']
                    ))
                else:
                    print(f"Record for {row['symbol']} on {row['date']} already exists. Skipping insert.")

            # Commit the transaction
            cursor.execute("COMMIT")

        except Exception as e:
            # Rollback the transaction if an error occurs
            cursor.execute("ROLLBACK")
            print(f"Error occurred: {e}")

        finally:
            cursor.close()
            conn.close()

    # Define task dependencies
    stock_data = fetch_stock_data()
    create_snowflake_table()
    load_table(stock_data)
