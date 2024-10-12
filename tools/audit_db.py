import sys
import os
import pandas as pd
from tabulate import tabulate
from sqlalchemy import create_engine

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.config_loader import ConfigLoader

def main():
    # Load configuration
    config = ConfigLoader.load_config()
    db_config = config['database']

    # Create SQLAlchemy engine
    db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@" \
             f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(db_url)

    try:
        # Fetch total count from raw_data
        raw_total_count_query = "SELECT COUNT(*) FROM raw_data;"
        raw_total_count = pd.read_sql_query(raw_total_count_query, engine).iloc[0, 0]
        print(f"\nTotal records in raw_data: {raw_total_count}")

        # Fetch counts per symbol from raw_data
        raw_counts_query = "SELECT symbol, COUNT(*) as count FROM raw_data GROUP BY symbol;"
        raw_counts_df = pd.read_sql_query(raw_counts_query, engine)
        print("\n=== Raw Data Counts per Symbol ===")
        print(tabulate(raw_counts_df, headers='keys', tablefmt='psql'))

        # Fetch raw data sample
        print("\n=== Raw Data Sample ===")
        raw_sample_query = "SELECT * FROM raw_data ORDER BY timestamp DESC LIMIT 10;"
        raw_sample_df = pd.read_sql_query(raw_sample_query, engine)
        if not raw_sample_df.empty:
            print(tabulate(raw_sample_df, headers='keys', tablefmt='psql'))
        else:
            print("No raw data found.")

        # Fetch total count from downsampled_data
        down_total_count_query = "SELECT COUNT(*) FROM downsampled_data;"
        down_total_count = pd.read_sql_query(down_total_count_query, engine).iloc[0, 0]
        print(f"\nTotal records in downsampled_data: {down_total_count}")

        # Fetch counts per symbol from downsampled_data
        down_counts_query = "SELECT symbol, COUNT(*) as count FROM downsampled_data GROUP BY symbol;"
        down_counts_df = pd.read_sql_query(down_counts_query, engine)
        print("\n=== Downsampled Data Counts per Symbol ===")
        print(tabulate(down_counts_df, headers='keys', tablefmt='psql'))

        # Fetch downsampled data sample
        print("\n=== Downsampled Data Sample ===")
        down_sample_query = "SELECT * FROM downsampled_data ORDER BY timestamp DESC LIMIT 10;"
        down_sample_df = pd.read_sql_query(down_sample_query, engine)
        if not down_sample_df.empty:
            print(tabulate(down_sample_df, headers='keys', tablefmt='psql'))
        else:
            print("No downsampled data found.")

        # Additional useful info: Time ranges
        # Raw data time range
        raw_time_range_query = "SELECT MIN(timestamp) AS start_time, MAX(timestamp) AS end_time FROM raw_data;"
        raw_time_range_df = pd.read_sql_query(raw_time_range_query, engine)
        print("\n=== Raw Data Time Range ===")
        print(tabulate(raw_time_range_df, headers='keys', tablefmt='psql', showindex=False))

        # Downsampled data time range
        down_time_range_query = "SELECT MIN(timestamp) AS start_time, MAX(timestamp) AS end_time FROM downsampled_data;"
        down_time_range_df = pd.read_sql_query(down_time_range_query, engine)
        print("\n=== Downsampled Data Time Range ===")
        print(tabulate(down_time_range_df, headers='keys', tablefmt='psql', showindex=False))

    except Exception as e:
        print(f"Error fetching data: {e}")
    finally:
        engine.dispose()

if __name__ == '__main__':
    main()
