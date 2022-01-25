import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def get_args():
    """parse command line arguments"""
    parser = argparse.ArgumentParser(description="Ingest CVS data to PostGreSQL.")
    parser.add_argument("--user", help="username for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table", help="table name for postgres")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()
    return args


def main(args):
    """ingest data into postgres"""
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table
    url = args.url
    csv_name = "data.csv"
    date_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    os.system(f"wget {url} -O {csv_name}")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df = pd.read_csv(csv_name, nrows=100, parse_dates=date_cols)
    schema = pd.io.sql.get_schema(df, name=table_name, con=engine)
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    df_iter = pd.read_csv(
        csv_name, iterator=True, chunksize=100000, parse_dates=date_cols
    )
    start = time()
    lap = start
    for df in df_iter:
        df.to_sql(name=table_name, con=engine, if_exists="append")
        print(f"Data chunk loaded to PG in {time() - lap:,.1f}s")
        lap = time()
    print(f"Total load time: {lap - start:,.1f}s")


if __name__ == "__main__":
    args = get_args()
    main(args)
