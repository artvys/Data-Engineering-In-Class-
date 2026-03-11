import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

SERVER = r"localhost\SQLEXPRESS"
DATABASE = "Tutorial7"
DRIVER = "ODBC Driver 17 for SQL Server"

SCHEMA = "dbo"
CLEAN_SCHEMA = "clean"
TABLE_NAME = "prd_info"

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
# TRANSFORM PRODUCT TABLE
# --------------------------------------------------
def transform_products():
    # Read the raw product table from dbo schema into a pandas DataFrame.
    # This is the original ingested version that we want to clean and transform.
    query = f"SELECT * FROM [{SCHEMA}].[{TABLE_NAME}]"
    df = pd.read_sql(query, engine)

    print(f"Original shape: {df.shape}")

    # 1. CLEAN TEXT COLUMNS
    # We first look at all columns with text/string data.
    # For each of them, we:
    # - remove leading spaces
    # - remove trailing spaces
    # - convert empty strings "" into real missing values 
    text_cols = df.select_dtypes(include="object").columns
    for col in text_cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace("", pd.NA)


    # 2. CONVERT DATE COLUMNS TO REAL DATES
    # prd_start_dt and prd_end_dt come from SQL / CSV as text-like values.
    # We convert them into actual datetime format so that we can work with them.
    df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors="coerce")
    df["prd_end_dt"] = pd.to_datetime(df["prd_end_dt"], errors="coerce")

    
    # 3. REMEMBER WHICH ROWS ORIGINALLY HAD AN END DATE
    # - if raw prd_end_dt is NULL, leave it NULL
    # - only adjust rows that already had a value
    df["had_end_date"] = df["prd_end_dt"].notna()

    
    # 4. CREATE prd_subcategory FROM THE FIRST 5 CHARACTERS OF prd_key
    df["prd_subcategory"] = df["prd_key"].str.replace("-", "_", regex=False).str[:5]

 
    # 5. REMOVE THOSE FIRST 5 CHARACTERS FROM prd_key
    df["prd_key"] = df["prd_key"].str[6:]

    # 6. MOVE prd_subcategory SO IT APPEARS RIGHT AFTER prd_key
    cols = df.columns.tolist()
    key_index = cols.index("prd_key")
    cols.insert(key_index + 1, cols.pop(cols.index("prd_subcategory")))
    df = df[cols]


    # 7. REPLACE NULL VALUES IN prd_cost WITH 0
    df["prd_cost"] = pd.to_numeric(df["prd_cost"], errors="coerce").fillna(0)

    # 8. STANDARDIZE prd_line VALUES
    # So we replace the coded values with full text labels,
    # then fill missing values with "Other".
    df["prd_line"] = df["prd_line"].replace({
        "R": "Road",
        "S": "Sport",
        "M": "Mountain",
        "T": "Touring"
    })
    df["prd_line"] = df["prd_line"].fillna("Other")

    
    # 9. UPDATE prd_end_dt BASED ON THE NEXT ROW'S prd_start_dt
    # Rule:
    # If the current row originally had an end date,
    # set its prd_end_dt to one day before the next row's prd_start_dt.
   
    # if the current row originally had NULL in prd_end_dt,
    # we must leave it NULL.
    for i in range(len(df) - 1):
        if df.loc[i, "had_end_date"] and pd.notna(df.loc[i + 1, "prd_start_dt"]):
            df.loc[i, "prd_end_dt"] = df.loc[i + 1, "prd_start_dt"] - pd.Timedelta(days=1)


    # 10. KEEP ORIGINAL NULL END DATES AS NULL.
    df.loc[~df["had_end_date"], "prd_end_dt"] = pd.NaT

    # The helper column was only needed during transformation,
    # so now we remove it from the final table.
    df = df.drop(columns=["had_end_date"])

    print(f"Cleaned shape: {df.shape}")


    # 11. WRITE CLEANED TABLE TO THE clean SCHEMA
    df.to_sql(
        TABLE_NAME,
        engine,
        schema=CLEAN_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"Created cleaned table: {CLEAN_SCHEMA}.{TABLE_NAME}")


with engine.begin() as conn:
    conn.execute(text("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'clean')
        BEGIN
            EXEC('CREATE SCHEMA clean')
        END
    """))


transform_products()

print("Done")