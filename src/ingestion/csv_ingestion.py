import pandas as pd
from src.database.connection import get_engine
import os


def fetch_data():
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "..", "..", "data", "raw", "Boston-house-price-data.csv")
    df = pd.read_csv(csv_path)
    return df


def transform_data(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )

    df.dropna(how="all", inplace=True)
    df.drop_duplicates(inplace=True)

    return df


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="kaggle_dataset",
        con=engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando ingestão do CSV...")

    df = fetch_data()
    df = transform_data(df)
    load_data(df)

    print("Ingestão finalizada!")


if __name__ == "__main__":
    run()