import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


# --------------------------------------------------
# CONNECTION SETTINGS
# --------------------------------------------------
SERVER = r"localhost\SQLEXPRESS"
DATABASE = "Tutorial7"
DRIVER = "ODBC Driver 17 for SQL Server"

SCHEMA = "dbo"
CLEAN_SCHEMA = "clean"
TABLE_NAME = "sales_details"


# --------------------------------------------------
# CREATE CONNECTION TO SQL SERVER
# --------------------------------------------------
odbc_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
    f"TrustServerCertificate=yes;"
)

engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str),
    fast_executemany=True
)


# --------------------------------------------------
# MAKE SURE CLEAN SCHEMA EXISTS
# --------------------------------------------------
def ensure_clean_schema():
    with engine.begin() as conn:
        conn.execute(text(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{CLEAN_SCHEMA}')
            BEGIN
                EXEC('CREATE SCHEMA {CLEAN_SCHEMA}')
            END
        """))


# --------------------------------------------------
# TRANSFORM SALES TABLE
# --------------------------------------------------
def transform_sales():
    # Read the raw sales table from the dbo schema.
    df = pd.read_sql(f"SELECT * FROM [{SCHEMA}].[{TABLE_NAME}]", engine)

    print(f"Original shape: {df.shape}")

    # --------------------------------------------------
    # 1. CLEAN TEXT COLUMNS
    # --------------------------------------------------
    # Remove leading/trailing spaces from text columns.
    # Also convert empty strings into proper missing values.
    text_cols = df.select_dtypes(include="object").columns
    for col in text_cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace("", pd.NA)

    # --------------------------------------------------
    # 2. CONVERT DATE COLUMNS TO REAL DATES
    # --------------------------------------------------
    # All date columns are converted into pandas datetime format.
    # Invalid values become NaT.
    df["sls_order_dt"] = pd.to_datetime(df["sls_order_dt"], format="%Y%m%d", errors="coerce")
    df["sls_ship_dt"] = pd.to_datetime(df["sls_ship_dt"], format="%Y%m%d", errors="coerce")
    df["sls_due_dt"] = pd.to_datetime(df["sls_due_dt"], format="%Y%m%d", errors="coerce")

    # --------------------------------------------------
    # 3. CONVERT NUMERIC COLUMNS TO NUMBERS
    # --------------------------------------------------
    # This makes sure numeric calculations work correctly.
    df["sls_sales"] = pd.to_numeric(df["sls_sales"], errors="coerce")
    df["sls_quantity"] = pd.to_numeric(df["sls_quantity"], errors="coerce")
    df["sls_price"] = pd.to_numeric(df["sls_price"], errors="coerce")

    # --------------------------------------------------
    # 4. REMOVE ROWS WHERE IMPORTANT IDS ARE NULL
    # --------------------------------------------------
    df = df.dropna(subset=["sls_ord_num", "sls_prd_key", "sls_cust_id"])

    # --------------------------------------------------
    # 5. FIX sls_price
    # --------------------------------------------------
    # Rule:
    # replace NULL or negative sls_price with:
    # sls_sales / sls_quantity from the same row
    #
    # We only do this when sls_quantity is not 0 and not null,
    # because division by zero is not allowed.
    invalid_price_mask = df["sls_price"].isna() | (df["sls_price"] < 0)
    valid_division_mask = df["sls_sales"].notna() & df["sls_quantity"].notna() & (df["sls_quantity"] != 0)

    df.loc[invalid_price_mask & valid_division_mask, "sls_price"] = (
        df.loc[invalid_price_mask & valid_division_mask, "sls_sales"] /
        df.loc[invalid_price_mask & valid_division_mask, "sls_quantity"]
    )

    # --------------------------------------------------
    # 6. FIX sls_sales
    # --------------------------------------------------
    # Rule:
    # replace NULL sls_sales with:
    # sls_price * sls_quantity
    #
    # This works after the price fix above.
    sales_null_mask = df["sls_sales"].isna()
    calc_sales_mask = df["sls_price"].notna() & df["sls_quantity"].notna()

    df.loc[sales_null_mask & calc_sales_mask, "sls_sales"] = (
        df.loc[sales_null_mask & calc_sales_mask, "sls_price"] *
        df.loc[sales_null_mask & calc_sales_mask, "sls_quantity"]
    )

    # --------------------------------------------------
    # 7. FIX sls_order_dt BASED ON ORDER NUMBER
    # --------------------------------------------------
    # Rule:
    # - if there are multiple items with the same order number,
    #   use one common order date for all rows in that order
    # - if there is only one item in that order,
    #   set order date to ship date minus one day
    #
    # For orders with multiple rows, we use the minimum existing order date
    # within that order as the common order date.
    order_counts = df.groupby("sls_ord_num")["sls_ord_num"].transform("count")
    min_order_dates = df.groupby("sls_ord_num")["sls_order_dt"].transform("min")

    # Multiple-item orders -> same shared order date for all rows
    df.loc[order_counts > 1, "sls_order_dt"] = min_order_dates[order_counts > 1]

    # Single-item orders -> ship date minus one day
    single_item_mask = order_counts == 1
    df.loc[single_item_mask & df["sls_ship_dt"].notna(), "sls_order_dt"] = (
        df.loc[single_item_mask & df["sls_ship_dt"].notna(), "sls_ship_dt"] - pd.Timedelta(days=1)
    )

    # --------------------------------------------------
    # 8. ENFORCE DATE ORDER
    # --------------------------------------------------
    # Rule:
    # order date should always be lower than ship date and due date.
    #
    # We fix obvious violations:
    # - if order_dt >= ship_dt, set order_dt = ship_dt - 1 day
    # - if order_dt >= due_dt, set order_dt = due_dt - 1 day
    #
    # This keeps order_dt earlier than both.
    mask_ship_problem = (
        df["sls_order_dt"].notna() &
        df["sls_ship_dt"].notna() &
        (df["sls_order_dt"] >= df["sls_ship_dt"])
    )
    df.loc[mask_ship_problem, "sls_order_dt"] = df.loc[mask_ship_problem, "sls_ship_dt"] - pd.Timedelta(days=1)

    mask_due_problem = (
        df["sls_order_dt"].notna() &
        df["sls_due_dt"].notna() &
        (df["sls_order_dt"] >= df["sls_due_dt"])
    )
    df.loc[mask_due_problem, "sls_order_dt"] = df.loc[mask_due_problem, "sls_due_dt"] - pd.Timedelta(days=1)

    # --------------------------------------------------
    # 9. OPTIONAL FINAL DATE CLEANUP
    # --------------------------------------------------
    # If ship date is later than due date, we make due date equal to ship date.
    # This was not explicitly required, but it avoids impossible timelines.
    mask_due_before_ship = (
        df["sls_ship_dt"].notna() &
        df["sls_due_dt"].notna() &
        (df["sls_ship_dt"] > df["sls_due_dt"])
    )
    df.loc[mask_due_before_ship, "sls_due_dt"] = df.loc[mask_due_before_ship, "sls_ship_dt"]

    # DROP EXTRA COLUMNS 
    # Remove ingestion tracking columns if they exist
    cols_to_drop = ["source_file", "source_path"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # --------------------------------------------------
    # 10. WRITE CLEANED TABLE TO clean SCHEMA
    # --------------------------------------------------
    # This creates a new cleaned table:
    # clean.sales_details
    #
    # The raw dbo.sales_details table stays unchanged.
    df.to_sql(
        TABLE_NAME,
        engine,
        schema=CLEAN_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"Created cleaned table: {CLEAN_SCHEMA}.{TABLE_NAME}")
    print(f"Cleaned shape: {df.shape}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    ensure_clean_schema()
    transform_sales()
    print("Transformation finished.")


if __name__ == "__main__":
    main()