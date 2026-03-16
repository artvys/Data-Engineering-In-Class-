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
TABLE_NAME = "LOC_A101"


# --------------------------------------------------
# CREATE CONNECTION
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
# TRANSFORM LOC_A101 TABLE
# --------------------------------------------------
def transform_locations():

    # Read raw table
    df = pd.read_sql(f"SELECT * FROM [{SCHEMA}].[{TABLE_NAME}]", engine)

    print(f"Original shape: {df.shape}")

    # --------------------------------------------------
    # 1. CLEAN TEXT COLUMNS
    # --------------------------------------------------
    # Remove leading/trailing spaces and convert empty strings to NULL
    text_cols = df.select_dtypes(include="object").columns
    for col in text_cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace("", pd.NA)

    # --------------------------------------------------
    # 2. FIX CID COLUMN
    # --------------------------------------------------
    # Leave only the last 5 characters so it matches customer keys.
    #
    # Example:
    # AW-00011000 -> 11000
    df["CID"] = df["CID"].str[-5:]

    # --------------------------------------------------
    # 3. CLEAN CNTRY COLUMN
    # --------------------------------------------------
    # Replace missing values with "N/A"
    df["CNTRY"] = df["CNTRY"].fillna("N/A")

    # Replace country codes with full country names
    df["CNTRY"] = df["CNTRY"].replace({
        "DE": "Germany",
        "US": "United States",
        "USA": "United States"
    })

    print(f"Cleaned shape: {df.shape}")

    # DROP EXTRA COLUMNS 
    # Remove ingestion tracking columns if they exist
    cols_to_drop = ["source_file", "source_path"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # --------------------------------------------------
    # 4. WRITE CLEANED TABLE
    # --------------------------------------------------
    # Creates clean.LOC_A101
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
    transform_locations()
    print("Transformation finished.")


if __name__ == "__main__":
    main()