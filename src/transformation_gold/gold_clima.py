import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM silver.clima", con=engine)

    return df


def transform_data(df):
    df["data_hora"] = pd.to_datetime(df["data_hora"], errors="coerce")
    df["hora"]      = df["data_hora"].dt.hour

    # Média geral de temperatura e vento
    resumo_geral = pd.DataFrame([{
        "temperatura_media_c":        df["temperatura_c"].mean().round(2),
        "temperatura_maxima_c":       df["temperatura_c"].max(),
        "temperatura_minima_c":       df["temperatura_c"].min(),
        "velocidade_media_vento_kmh": df["velocidade_vento_kmh"].mean().round(2),
        "total_registros":            len(df),
    }])

    return resumo_geral


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="clima_resumo",
        con=engine,
        schema="gold",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando Gold: clima...")

    df = fetch_data()
    df = transform_data(df)
    load_data(df)

    print("Gold clima finalizado!")


if __name__ == "__main__":
    run()
