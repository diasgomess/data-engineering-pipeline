import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM bronze.livros", con=engine)

    return df


def transform_data(df):
    df.dropna(subset=["title", "author_name"], inplace=True)

    # Tipos corretos
    df["first_publish_year"]      = pd.to_numeric(df["first_publish_year"], errors="coerce").astype("Int64")
    df["number_of_pages_median"]  = pd.to_numeric(df["number_of_pages_median"], errors="coerce").astype("Int64")
    df["edition_count"]           = pd.to_numeric(df["edition_count"], errors="coerce").astype("Int64")
    df["ratings_count"]           = pd.to_numeric(df["ratings_count"], errors="coerce").astype("Int64")
    df["ratings_average"]         = pd.to_numeric(df["ratings_average"], errors="coerce").round(2)
    df["want_to_read_count"]      = pd.to_numeric(df["want_to_read_count"], errors="coerce").astype("Int64")
    df["currently_reading_count"] = pd.to_numeric(df["currently_reading_count"], errors="coerce").astype("Int64")
    df["already_read_count"]      = pd.to_numeric(df["already_read_count"], errors="coerce").astype("Int64")

    # Filtra anos inválidos (muito antigos ou no futuro)
    df = df[df["first_publish_year"].between(1000, 2025, inclusive="both")]

    # Trim em texto
    df["title"]       = df["title"].str.strip()
    df["author_name"] = df["author_name"].str.strip()

    # Remove duplicatas pela chave natural
    df.drop_duplicates(subset=["key"], inplace=True)

    return df


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="livros",
        con=engine,
        schema="silver",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando Silver: livros...")

    df = fetch_data()
    df = transform_data(df)
    load_data(df)

    print("Silver livros finalizado!")


if __name__ == "__main__":
    run()
