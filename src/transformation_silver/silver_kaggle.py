import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM bronze.kaggle_dataset", con=engine)

    return df


def transform_data(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )

    # Remove linhas e colunas 100% vazias
    df.dropna(how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    # Remove duplicatas
    df.drop_duplicates(inplace=True)

    # Inferência de tipos
    df = df.infer_objects()

    # Converte colunas que parecem numéricas mas vieram como texto
    for col in df.select_dtypes(include="object").columns:
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().sum() > len(df) * 0.8:  # mais de 80% converteu → é numérica
            df[col] = converted

    return df


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="kaggle_dataset",
        con=engine,
        schema="silver",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando Silver: kaggle_dataset...")

    df = fetch_data()
    df = transform_data(df)
    load_data(df)

    print("Silver kaggle_dataset finalizado!")


if __name__ == "__main__":
    run()
