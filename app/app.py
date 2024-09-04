import pandas as pd
import re
from datetime import datetime
from helper import *

def main():
    path = "https://docs.google.com/spreadsheets/d/1P-vSxpMtPWGSGDCTO7ydzuAu3468mWfrsYnhvkBKgB0/edit?gid=313510953#gid=313510953"
    pa = path.split("?")
    id_ = pa[1].split("#")
    pa2 = re.split("edit", pa[0], 1)
    format = "export?format=csv&"
    fullpath = pa2[0] + format + id_[1]
    print(fullpath)
    df = pd.read_csv(fullpath)
    df.index = df.index + 1
    df.reset_index(inplace=True)
    df["movies_id"] = df["index"]
    df.drop(columns="index", inplace=True)

    new_order = [df.columns[-1]] + df.columns[:-1].tolist()
    df = df[new_order]
    df = infer_type_date(df)
    df['create_at'] = datetime.now()

    print(df.info())
    
    df = fill_zero_number(df, "int64")
    df = fill_zero_number(df, "float64")

    create_schema(df, "movies", "movies_id")
    delete_data("movies")
    insert_data(df, "movies")
if __name__ == "__main__":
    main()
