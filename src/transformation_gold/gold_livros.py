import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM silver.livros", con=engine)

    return df


def transform_data(df):
    # Top 10 livros mais bem avaliados (mínimo 10 avaliações)
    top_avaliados = (
        df[df["ratings_count"] >= 10]
        .sort_values("ratings_average", ascending=False)
        .head(10)
        [["title", "author_name", "ratings_average", "ratings_count", "first_publish_year"]]
        .reset_index(drop=True)
    )
    top_avaliados["ranking"] = top_avaliados.index + 1

    return top_avaliados


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="livros_top_avaliados",
        con=engine,
        schema="gold",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando Gold: livros...")

    df = fetch_data()
    df = transform_data(df)
    load_data(df)

    print("Gold livros finalizado!")


if __name__ == "__main__":
    run()
