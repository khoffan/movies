import pandas as pd
from helper import *

def main():
    df = pd.read_csv(r"D:\Technical\python_project\data_sci\tutorial_data\Database_server\mock_movies.csv")
    df.index = df.index + 1
    df.reset_index(inplace=True)
    df["movies_id"] = df["index"]
    df.drop(columns="index", inplace=True)

    new_order = [df.columns[-1]] + df.columns[:-1].tolist()
    df = df[new_order]
    df = infer_type_date(df)

    create_schema(df, "movies", "movies_id")
    delete_data("movies")
    insert_data(df, "movies")
if __name__ == "__main__":
    main()