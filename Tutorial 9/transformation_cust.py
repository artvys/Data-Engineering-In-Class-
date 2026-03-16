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
TABLE_NAME = "CUST_AZ12"


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
# TRANSFORM CUST_AZ12 TABLE
# --------------------------------------------------
def transform_cust_az12():
    # Read the raw table from dbo schema
    df = pd.read_sql(f"SELECT * FROM [{SCHEMA}].[{TABLE_NAME}]", engine)

    print(f"Original shape: {df.shape}")

    # --------------------------------------------------
    # 1. CLEAN TEXT COLUMNS
    # --------------------------------------------------
    # Remove leading/trailing spaces from text columns
    # and turn empty strings into missing values
    text_cols = df.select_dtypes(include="object").columns
    for col in text_cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace("", pd.NA)

    # --------------------------------------------------
    # 2. FIX CID
    # --------------------------------------------------
    
    # Example:
    # NASAW00011000 -> 11000
    df["CID"] = df["CID"].str[-5:]

    # Optional: if you want it numeric like cust_info.cst_key often is,
    # uncomment the next line:
    # df["CID"] = pd.to_numeric(df["CID"], errors="coerce")

    # --------------------------------------------------
    # 3. FIX GEN
    # --------------------------------------------------
    # Where gender is missing, replace with "n/a"
    df["GEN"] = df["GEN"].fillna("n/a")

    # --------------------------------------------------
    # 4. FIX BDATE
    # --------------------------------------------------
    # Convert BDATE into real date format.
    # Invalid dates become NaT.
    df["BDATE"] = pd.to_datetime(df["BDATE"], errors="coerce")

    # If someone has a birthdate in the future,
    # replace it with NULL (NaT in pandas).
    today = pd.Timestamp.today().normalize()
    df.loc[df["BDATE"] > today, "BDATE"] = pd.NaT

    print(f"Cleaned shape: {df.shape}")

    # DROP EXTRA COLUMNS 
    # Remove ingestion tracking columns if they exist
    cols_to_drop = ["source_file", "source_path"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # --------------------------------------------------
    # 5. WRITE CLEANED TABLE TO clean SCHEMA
    # --------------------------------------------------
    # This creates:
    # clean.CUST_AZ12
    #
    # Raw dbo.CUST_AZ12 stays unchanged.
    df.to_sql(
        TABLE_NAME,
        engine,
        schema=CLEAN_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"Created cleaned table: {CLEAN_SCHEMA}.{TABLE_NAME}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    ensure_clean_schema()
    transform_cust_az12()
    print("Transformation finished.")


if __name__ == "__main__":
    main()