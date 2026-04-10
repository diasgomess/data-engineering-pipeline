import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM silver.kaggle_dataset", con=engine)

    return df


def transform_data(df):
    # Seleciona apenas colunas numéricas para agregação
    num_cols = df.select_dtypes(include="number").columns.tolist()

    # Resumo estatístico: média, máximo, mínimo e contagem por coluna
    resumo = pd.DataFrame({
        "coluna":    num_cols,
        "media":     [df[c].mean().round(2) for c in num_cols],
        "maximo":    [df[c].max()           for c in num_cols],
        "minimo":    [df[c].min()           for c in num_cols],
        "nulos":     [df[c].isna().sum()    for c in num_cols],
        "total":     [df[c].count()         for c in num_cols],
    })

    return resumo


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="kaggle_resumo_estatistico",
        con=engine,
        schema="gold",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando Gold: kaggle_dataset...")

    df = fetch_data()
    df = transform_data(df)
    load_data(df)

    print("Gold kaggle_dataset finalizado!")


if __name__ == "__main__":
    run()
