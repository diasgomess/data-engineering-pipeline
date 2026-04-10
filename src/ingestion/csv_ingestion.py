import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    df = pd.read_csv("csv\\Boston-house-price-data.csv")
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