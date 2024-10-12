# tools/clear_db.py

import argparse
from sqlalchemy import create_engine, text
import sys
import os

# Adjust the Python path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.config_loader import ConfigLoader

def clear_tables(raw=False, downsampled=False):
    # Load configuration
    config = ConfigLoader.load_config()
    db_config = config['database']

    # Create SQLAlchemy engine
    db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@" \
             f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(db_url)

    try:
        with engine.begin() as connection:
            if raw:
                connection.execute(text("TRUNCATE TABLE raw_data RESTART IDENTITY;"))
                print("raw_data table cleared.")
            if downsampled:
                connection.execute(text("TRUNCATE TABLE downsampled_data RESTART IDENTITY;"))
                print("downsampled_data table cleared.")
    except Exception as e:
        print(f"Error clearing tables: {e}")
    finally:
        engine.dispose()

def main():
    parser = argparse.ArgumentParser(description='Clear database tables.')
    parser.add_argument('--raw', action='store_true', help='Clear raw_data table.')
    parser.add_argument('--downsampled', action='store_true', help='Clear downsampled_data table.')
    args = parser.parse_args()

    if not args.raw and not args.downsampled:
        print("Please specify at least one table to clear using --raw and/or --downsampled.")
        return

    clear_tables(raw=args.raw, downsampled=args.downsampled)

if __name__ == '__main__':
    main()
