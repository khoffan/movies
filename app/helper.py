from sqlalchemy import Integer, DateTime, String, Float, Column ,Table, MetaData, Text
from sqlalchemy import ForeignKey, inspect, insert, select, delete, func, exc
from sqlalchemy.orm import sessionmaker, relationship
import re
import pandas as pd
from dbConfig import *

engine = connect_db()
conn = engine.connect()
session = sessionmaker(bind=engine)()
metadata = MetaData()


def infer_type_date(df):
    pattrens = ["^\d{1,4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}$", "^\d{1,4}/\d{1,2}/\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}$", "^\d{1,4}-\d{1,2}-\d{1,2}$", "^\d{1,4}/\d{1,2}/\d{1,2}$","^\d{1,2}/\d{1,2}/\d{1,4}$", "^\d{1,4}-\d{1,2}-\d{1,2}\w\d{1,2}:\d{1,2}:\d{1,2}.\d{1,3}\w$"]
    for col in df.columns:
        def match_date(val):
            if val is None:
                return False
            return any(re.search(pattern, str(val), re.IGNORECASE) for pattern in pattrens)
        if any(df[col].apply(match_date)):
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].where(pd.notnull(df[col]), None)

    return df


def fill_zero_number(df, type):
    for col in df.columns:
        if df[col].dtype == type:
            df[col] = df[col].fillna(0)
    return df

def check_table_exist(table_name):
    try:
        if not inspect(engine).has_table(table_name):
            return False
        else:
            return True
    except Exception as e:
        print(f"An error occurred check table exist: {str(e)}")
        return "failure"
    finally:
        engine.dispose()


def check_data_exists(table_name):
    try:
        table = Table(table_name, metadata, autoload_with=engine)
        stmt = select(func.count()).select_from(table)
        result = session.execute(stmt)
        count = result.scalar()
        if count > 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"An error occurred check data exist: {str(e)}")
        return "failure"
    finally:
        engine.dispose()
def create_schema(df, table_name, primary_key):
    try:
        check_table = check_table_exist(table_name)
        if check_table:
            print("table already exists")
            return "table already exists"
        columns = []
        for col in df.columns:
            if df[col].dtype == 'int64':
                col_type = Integer
            elif df[col].dtype == 'float64':
                col_type = Float
            elif df[col].dtype == 'datetime64[ns]':
                col_type = DateTime
            else:
                col_type = Text

            if col == primary_key:
                columns.append(Column(col, col_type, primary_key=True))
            else:
                columns.append(Column(col, col_type))
        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)
        return table
    except Exception as e:
        print(str(e))
    finally:
        engine.dispose()


def insert_data(df, table_name):
    try:
        count = check_data_exists(table_name)
        if count:
            print("data already exists")
            return "data already exists"
        df.to_sql(table_name, con=conn, if_exists='append', index=False)
        print("insert success")
        return "insert success"
    except Exception as e:
        print(f"An error occurred insert data: {e}")
        return "failure"
    finally:
        engine.dispose()

def delete_data(table_name):
    try:
        table = Table(table_name, metadata, autoload_with=engine)
        stmt = delete(table)
        session.execute(stmt)
        session.commit()
        print("delete success")
        return "delete success"
    except Exception as e:
        session.rollback()
        print(f"An error occurred delete data: {e}")
        return "failure"
    finally:
        session.close()
        engine.dispose()
