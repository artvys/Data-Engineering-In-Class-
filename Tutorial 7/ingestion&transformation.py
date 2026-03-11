import os
import re
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


# CONFIG
SERVER = r"localhost\SQLEXPRESS"
DRIVER = "ODBC Driver 17 for SQL Server"
DB_NAME = "Tutorial7"

USE_SQL_LOGIN = False
USERNAME = ""
PASSWORD = ""

SCHEMA = "dbo"

CSV_FOLDERS = [
    r"C:\Users\vysot\Desktop\U.M\Data Engineering\In-Class project\datasets\source_crm",
    r"C:\Users\vysot\Desktop\U.M\Data Engineering\In-Class project\datasets\source_erp",
]

TABLE_FROM_FILENAME_ONLY = True
IF_EXISTS = "replace"
CHUNKSIZE = 2000

# Transformation config
CLEAN_SCHEMA = "clean"
CUSTOMERS_TABLE = "cust_info"


def make_odbc_str(database: str) -> str:
    return (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )


def safe_table_name(name: str) -> str:
    name = os.path.splitext(name)[0]
    name = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    if not name:
        name = "table"
    if re.match(r"^\d", name):
        name = "t_" + name
    return name[:128]


def create_database_if_missing():
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

# ----- THIS PART IS FOR CLEANING -----
def clean_customers_data(df):
    # Strip spaces from text columns
    text_cols = df.select_dtypes(include=["object"]).columns
    for col in text_cols:
        df[col] = df[col].str.strip()

    # Turn empty strings into missing values
    for col in text_cols:
        df[col] = df[col].replace("", pd.NA)

    # Remove rows where customer id is missing
    df = df.dropna(subset=["cst_id"])

    # Remove duplicates on customer id, keep last
    df = df.drop_duplicates(subset=["cst_id"], keep="last")

    # Standardize marital status
    if "cst_marital_status" in df.columns:
        df["cst_marital_status"] = df["cst_marital_status"].replace({
            "M": "Married",
            "S": "Single"
        })
        df["cst_marital_status"] = df["cst_marital_status"].fillna("N/A")

    # Standardize gender
    if "cst_gndr" in df.columns:
        df["cst_gndr"] = df["cst_gndr"].replace({
            "M": "Male",
            "F": "Female"
        })
        df["cst_gndr"] = df["cst_gndr"].fillna("N/A")

    # Convert cst_id to integer if possible
    if "cst_id" in df.columns:
        df["cst_id"] = df["cst_id"].astype(int)

    return df

# ----- THIS PART IS FOR TRANSFORMATION -----
def transform_customers(engine):
    print("\nStarting transformation for cust_info...")

    # Read original customer table
    query = f"SELECT * FROM [{SCHEMA}].[{CUSTOMERS_TABLE}]"
    df = pd.read_sql(query, engine)

    print(f"Original {SCHEMA}.{CUSTOMERS_TABLE} shape: {df.shape}")

    # Clean the data
    cleaned_df = clean_customers_data(df.copy())

    print(f"Cleaned {CLEAN_SCHEMA}.{CUSTOMERS_TABLE} shape: {cleaned_df.shape}")

    with engine.begin() as conn:
        # Create clean schema if it does not exist
        conn.execute(text(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{CLEAN_SCHEMA}')
            BEGIN
                EXEC('CREATE SCHEMA {CLEAN_SCHEMA}')
            END
        """))

    # Save cleaned data to clean schema
    cleaned_df.to_sql(
        name=CUSTOMERS_TABLE,
        con=engine,
        schema=CLEAN_SCHEMA,
        if_exists="replace",
        index=False,
        chunksize=CHUNKSIZE,
        method=None,
    )

    print(f"Cleaned data written to {CLEAN_SCHEMA}.{CUSTOMERS_TABLE}")

    # Replace old dbo.cust_info with cleaned data
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE [{SCHEMA}].[{CUSTOMERS_TABLE}]"))

    cleaned_df.to_sql(
        name=CUSTOMERS_TABLE,
        con=engine,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        chunksize=CHUNKSIZE,
        method=None,
    )

    print(f"{SCHEMA}.{CUSTOMERS_TABLE} replaced with cleaned data")



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
            folder_part = safe_table_name(os.path.basename(os.path.dirname(csv_path)))
            table = safe_table_name(f"{folder_part}_{base}")

        df = pd.read_csv(csv_path)
        df = df.replace({"": None})

        df["source_file"] = base
        df["source_path"] = csv_path

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

    print("\nIngestion finished.")

    # 4) Transform Customers table
    transform_customers(engine)

    print("\nAll done.")


if __name__ == "__main__":
    main()