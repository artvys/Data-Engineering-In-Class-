import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


SERVER = r"localhost\SQLEXPRESS"
DATABASE = "Tutorial7"
DRIVER = "ODBC Driver 17 for SQL Server"

CLEAN_SCHEMA = "clean"
CURATED_SCHEMA = "curated"


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


def ensure_curated_schema():
    with engine.begin() as conn:
        conn.execute(text(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{CURATED_SCHEMA}')
            BEGIN
                EXEC('CREATE SCHEMA {CURATED_SCHEMA}')
            END
        """))


def create_dim_customers():
    cust_info = pd.read_sql(f"SELECT * FROM [{CLEAN_SCHEMA}].[cust_info]", engine)
    cust_az12 = pd.read_sql(f"SELECT * FROM [{CLEAN_SCHEMA}].[CUST_AZ12]", engine)
    loc_a101 = pd.read_sql(f"SELECT * FROM [{CLEAN_SCHEMA}].[LOC_A101]", engine)

    # Remove ingestion columns if they still exist
    cols_to_drop = ["source_file", "source_path"]
    cust_info = cust_info.drop(columns=[c for c in cols_to_drop if c in cust_info.columns])
    cust_az12 = cust_az12.drop(columns=[c for c in cols_to_drop if c in cust_az12.columns])
    loc_a101 = loc_a101.drop(columns=[c for c in cols_to_drop if c in loc_a101.columns])

    # Standardize join keys AGAIN before joining
    cust_info["cst_key"] = cust_info["cst_key"].astype(str).str.strip().str[-5:]
    cust_az12["CID"] = cust_az12["CID"].astype(str).str.strip().str[-5:]
    loc_a101["CID"] = loc_a101["CID"].astype(str).str.strip().str[-5:]

    # Join customer info with demographics
    dim_customers = cust_info.merge(
        cust_az12,
        how="left",
        left_on="cst_key",
        right_on="CID"
    )

    # Join with location
    dim_customers = dim_customers.merge(
        loc_a101,
        how="left",
        left_on="cst_key",
        right_on="CID"
    )

    # Create one final gender column
    # Prefer GEN from CUST_AZ12, otherwise use cst_gndr from cust_info
    dim_customers["gender"] = dim_customers["GEN"].fillna(dim_customers["cst_gndr"]).fillna("N/A")

    # Fill missing country values after join
    dim_customers["CNTRY"] = dim_customers["CNTRY"].fillna("N/A")

    # Drop duplicate / helper columns
    dim_customers = dim_customers.drop(columns=[
        c for c in ["CID_x", "CID_y", "GEN", "cst_gndr"] if c in dim_customers.columns
    ])

    # Rename columns
    dim_customers = dim_customers.rename(columns={
        "cst_id": "customer_id",
        "cst_key": "customer_key",
        "cst_firstname": "first_name",
        "cst_lastname": "last_name",
        "cst_marital_status": "marital_status",
        "cst_create_date": "create_date",
        "BDATE": "birth_date",
        "CNTRY": "country"
    })

    # Reorder columns
    wanted_order = [
        "customer_id",
        "customer_key",
        "first_name",
        "last_name",
        "marital_status",
        "gender",
        "birth_date",
        "country",
        "create_date"
    ]
    dim_customers = dim_customers[[c for c in wanted_order if c in dim_customers.columns]]

    dim_customers.to_sql(
        "dim_customers",
        engine,
        schema=CURATED_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"Created curated table: {CURATED_SCHEMA}.dim_customers")
    print(f"Shape: {dim_customers.shape}")


def create_dim_products():
    prd_info = pd.read_sql(f"SELECT * FROM [{CLEAN_SCHEMA}].[prd_info]", engine)

    cols_to_drop = ["source_file", "source_path"]
    prd_info = prd_info.drop(columns=[c for c in cols_to_drop if c in prd_info.columns])

    # Optional cleanup in case there are still spaces
    text_cols = prd_info.select_dtypes(include="object").columns
    for col in text_cols:
        prd_info[col] = prd_info[col].str.strip()

    # Rename columns to dimension-style names
    dim_products = prd_info.rename(columns={
        "prd_id": "product_id",
        "prd_key": "product_key",
        "prd_subcategory": "subcategory",
        "prd_nm": "product_name",
        "prd_cost": "product_cost",
        "prd_line": "product_line",
        "prd_start_dt": "start_date",
        "prd_end_dt": "end_date"
    })

    wanted_order = [
        "product_id",
        "product_key",
        "subcategory",
        "product_name",
        "product_cost",
        "product_line",
        "start_date",
        "end_date"
    ]
    dim_products = dim_products[[c for c in wanted_order if c in dim_products.columns]]

    dim_products.to_sql(
        "dim_products",
        engine,
        schema=CURATED_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"Created curated table: {CURATED_SCHEMA}.dim_products")
    print(f"Shape: {dim_products.shape}")


def create_fact_sales():
    sales = pd.read_sql(f"SELECT * FROM [{CLEAN_SCHEMA}].[sales_details]", engine)

    cols_to_drop = ["source_file", "source_path"]
    sales = sales.drop(columns=[c for c in cols_to_drop if c in sales.columns])

    # Clean possible leftover spaces in text columns
    text_cols = sales.select_dtypes(include="object").columns
    for col in text_cols:
        sales[col] = sales[col].str.strip()

    # Standardize keys so they match the curated dimensions
    sales["sls_cust_id"] = sales["sls_cust_id"].astype(str).str.strip().str[-5:]
    sales["sls_prd_key"] = sales["sls_prd_key"].astype(str).str.strip()

    # Rename columns into fact-style names
    fact_sales = sales.rename(columns={
        "sls_ord_num": "order_number",
        "sls_prd_key": "product_key",
        "sls_cust_id": "customer_key",
        "sls_order_dt": "order_date",
        "sls_ship_dt": "ship_date",
        "sls_due_dt": "due_date",
        "sls_sales": "sales_amount",
        "sls_quantity": "quantity",
        "sls_price": "price"
    })

    wanted_order = [
        "order_number",
        "product_key",
        "customer_key",
        "order_date",
        "ship_date",
        "due_date",
        "sales_amount",
        "quantity",
        "price"
    ]
    fact_sales = fact_sales[[c for c in wanted_order if c in fact_sales.columns]]

    fact_sales.to_sql(
        "fact_sales",
        engine,
        schema=CURATED_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"Created curated table: {CURATED_SCHEMA}.fact_sales")
    print(f"Shape: {fact_sales.shape}")

def main():
    ensure_curated_schema()
    create_dim_customers()
    create_dim_products()
    create_fact_sales()
    print("Curation finished.")


if __name__ == "__main__":
    main()