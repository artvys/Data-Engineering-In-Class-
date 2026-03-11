import os
import re
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus


# CONFIG 
SERVER = r"localhost\SQLEXPRESS"        
DRIVER = "ODBC Driver 17 for SQL Server"  
DB_NAME = "Tutorial6"

# Use SQL Login:
USE_SQL_LOGIN = False
USERNAME = ""
PASSWORD = ""

SCHEMA = "dbo"

# CSVs
CSV_FOLDERS = [
    r"C:\Users\vysot\Desktop\U.M\Data Engineering\In-Class project\datasets\source_crm",
    r"C:\Users\vysot\Desktop\U.M\Data Engineering\In-Class project\datasets\source_erp",
]

# If True: table = filename 
# If False: table = a safe unique name including folder hash-ish
TABLE_FROM_FILENAME_ONLY = True

# What to do if table already exists: "replace" or "append"
IF_EXISTS = "replace"

CHUNKSIZE = 2000
# -----------------------


def make_odbc_str(database: str) -> str:
    return (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )


def safe_table_name(name: str) -> str:
    # SQL Server friendly table name (letters/numbers/_), no leading digits
    name = os.path.splitext(name)[0]
    name = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    if not name:
        name = "table"
    if re.match(r"^\d", name):
        name = "t_" + name
    return name[:128]  # SQL Server identifier limit is 128


def create_database_if_missing():
    # Create DB via pyodbc on master (autocommit required for CREATE DATABASE)
    master_conn_str = make_odbc_str("master")
    with pyodbc.connect(master_conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(f"IF DB_ID(N'{DB_NAME}') IS NULL CREATE DATABASE [{DB_NAME}];")
    print(f"Database ensured: {DB_NAME}")


def get_engine_for_database(db: str):
    odbc_str = make_odbc_str(db)
    url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str)
    return create_engine(url, fast_executemany=True)


def iter_csv_files(folders):
    for folder in folders:
        for root, _, files in os.walk(folder):
            for fn in files:
                if fn.lower().endswith(".csv"):
                    yield os.path.join(root, fn)


def main():
    # 1) Create DB
    create_database_if_missing()

    # 2) Engine to Tutorial5
    engine = get_engine_for_database(DB_NAME)

    # 3) Load CSVs
    csv_files = list(iter_csv_files(CSV_FOLDERS))
    if not csv_files:
        print("No CSV files found in the provided folders.")
        return

    print(f"Found {len(csv_files)} CSV file(s). Starting ingestion...")

    for csv_path in csv_files:
        base = os.path.basename(csv_path)

        if TABLE_FROM_FILENAME_ONLY:
            table = safe_table_name(base)
        else:
            # make table name include folder name to reduce collisions
            folder_part = safe_table_name(os.path.basename(os.path.dirname(csv_path)))
            table = safe_table_name(f"{folder_part}_{base}")

        # Read CSV
        df = pd.read_csv(csv_path)
        df = df.replace({"": None})  # optional cleanup

        # Optional: track origin
        df["source_file"] = base
        df["source_path"] = csv_path

        # Write to SQL Server
        df.to_sql(
            name=table,
            con=engine,
            schema=SCHEMA,
            if_exists=IF_EXISTS,
            index=False,
            chunksize=CHUNKSIZE,
            method=None,
        )

        print(f"Loaded {csv_path} -> {SCHEMA}.{table} ({len(df)} rows, if_exists={IF_EXISTS})")

    print("All done.")


if __name__ == "__main__":
    main()