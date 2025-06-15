import argparse
import chardet
import json
import os
import pandas as pd
from dateutil import parser
from dotenv import load_dotenv
from sqlalchemy import DATE, TIMESTAMP, create_engine, MetaData, Table, Column, text, inspect
from sqlalchemy.types import String, Integer, Float, DateTime, Boolean
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, FLOAT, BOOLEAN, DATETIME

# --- Config ---
MAPPING_FILE = "resources/mappings/mapping.json"

# Load environment variables from .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")
DB_DIALECT = os.getenv("DB_DIALECT")
DB_DRIVER = os.getenv("DB_DRIVER")

DB_CONNECTION_STRING = f"{DB_DIALECT}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

import numpy as np
import pandas as pd

def map_dtype(dtype, col_data=None):
    if "int" in str(dtype):
        return Integer
    elif "float" in str(dtype):
        return Float
    elif "bool" in str(dtype):
        return Boolean
    elif "datetime" in str(dtype):
        return DateTime
    elif "object" in str(dtype):
        if col_data is not None:
            try:
                max_len = col_data.dropna().str.len().max()
                return String(min(max_len or 1, 1000))
            except Exception:
                return String(255)
        return String(255)
    return String(255)

def map_sql_type_to_dtype(sql_type):
    if isinstance(sql_type, VARCHAR):
        return str
    elif isinstance(sql_type, INTEGER):
        return np.int64
    elif isinstance(sql_type, FLOAT):
        return np.float64
    elif isinstance(sql_type, BOOLEAN):
        return bool
    elif isinstance(sql_type, DATETIME):
        return 'datetime64[ns]'
    elif isinstance(sql_type, DATE):
        return 'datetime64[ns]'
    elif isinstance(sql_type, TIMESTAMP):
        return 'datetime64[ns]'
    else:
        return str

def clean_date_column(df, column_name='date'):
    # Step 1: Remove non-breaking space, strip whitespace
    df[column_name] = (
        df[column_name]
        .astype(str)
        .str.replace(u'\xa0', ' ', regex=False)
        .str.strip()
    )
    
    # Step 2: Parse to datetime (handles MM/DD/YYYY too)
    df[column_name] = pd.to_datetime(
        df[column_name], 
        errors='coerce'
    )
    
    # Step 3: Convert to string format 'YYYY-MM-DD' for MySQL
    df[column_name] = df[column_name].dt.strftime('%Y-%m-%d')

    return df[column_name]

def try_parse_date(x):
    try:
        return parser.parse(x)
    except Exception:
        return pd.NaT

def cast_dataframe_to_table_schema(df, engine, table_name):
    insp = inspect(engine)
    if not insp.has_table(table_name):
        raise ValueError(f"Table '{table_name}' does not exist.")

    db_columns = insp.get_columns(table_name)
    typed_data = {}

    for col in db_columns:
        col_name = col["name"]
        col_type = col["type"]

        if col_name in df.columns:
            target_dtype = map_sql_type_to_dtype(col_type)
            # print(col_name,target_dtype)
            try:
                if target_dtype == 'datetime64[ns]':
                    typed_data[col_name] = clean_date_column(df, col_name)
                else:
                    typed_data[col_name] = df[col_name].astype(target_dtype)
            except Exception as e:
                raise ValueError(f"Error casting '{col_name}' to {target_dtype}: {e}")
        else:
            print(f"'{col_name}' missing in CSV. Filling with None.")
            typed_data[col_name] = None

    return pd.DataFrame(typed_data)

def load_mapping():
    with open(MAPPING_FILE, "r") as f:
        return json.load(f)

def load_csv_to_table(file_name, table_name, wipe_and_load):
    print(f"Loading CSV: {file_name} into table: {table_name} (Wipe: {wipe_and_load})")
    
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"CSV file not found: {file_name}")

    with open(file_name, 'rb') as f:
        result = chardet.detect(f.read(10000))  # read sample
        encoding = result['encoding']
        print(f"Detected encoding: {encoding}")

    df = pd.read_csv(file_name, encoding=encoding)
    # Strip \xa0 and surrounding whitespace from all string cells
    df = df.map(lambda x: str(x).replace('\xa0', ' ').strip() if isinstance(x, str) else x)
    engine = create_engine(DB_CONNECTION_STRING)
    metadata = MetaData()

    if not inspect(engine).has_table(table_name):
        print(f"Creating table: {table_name}")
        columns = []
        for col in df.columns:
            dtype = df[col].dtype
            sqlalchemy_type = map_dtype(dtype, df[col])
            columns.append(Column(col, sqlalchemy_type))
        # columns = [Column(col, map_dtype(dtype)) for col, dtype in zip(df.columns, df.dtypes)]
        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)
        print(f"Table created: {table_name}")
    elif wipe_and_load:
        with engine.connect() as conn2:
            print(f"Truncating table: {table_name}")
            conn2.execute(text(f"SET FOREIGN_KEY_CHECKS=0;"))
            conn2.execute(text(f"TRUNCATE TABLE `{table_name}`;"))
            conn2.execute(text(f"SET FOREIGN_KEY_CHECKS=1;"))

    print(f"Data loading into: {table_name}")
    df_casted = cast_dataframe_to_table_schema(df, engine, table_name)
    df_casted.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Data successfully loaded into: {table_name}")

def main():
    parser = argparse.ArgumentParser(description="CSV to DB loader using mapping")
    parser.add_argument("--table", help="CSV file name as key in mapping file")
    parser.add_argument("--override_wipe", choices=["true", "false"], help="Override mapping wipe_and_load")

    args = parser.parse_args()
    mapping = load_mapping()

    if args.table not in mapping:
        raise ValueError(f"No mapping entry found for: {args.table}")

    entry = mapping[args.table]
    file_name = entry["file_name"]
    table_name = entry["table_name"]
    wipe_and_load = entry.get("wipe_and_load", False)

    if args.override_wipe is not None:
        wipe_and_load = args.override_wipe.lower() == "true"

    load_csv_to_table(file_name, table_name, wipe_and_load)

if __name__ == "__main__":
    main()
