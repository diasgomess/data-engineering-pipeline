import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM bronze.clima", con=engine)

    return df


def transform_data(df):
    df = df.rename(columns={
        "time":           "data_hora",
        "temperature":    "temperatura_c",
        "windspeed":      "velocidade_vento_kmh",
        "winddirection":  "direcao_vento_graus",
        "weathercode":    "codigo_tempo",
        "is_day":         "eh_dia",
    })

    # Tipos corretos
    df["data_hora"]             = pd.to_datetime(df["data_hora"], errors="coerce")
    df["temperatura_c"]         = pd.to_numeric(df["temperatura_c"], errors="coerce")
    df["velocidade_vento_kmh"]  = pd.to_numeric(df["velocidade_vento_kmh"], errors="coerce")
    df["direcao_vento_graus"]   = pd.to_numeric(df["direcao_vento_graus"], errors="coerce")
    df["codigo_tempo"]          = pd.to_numeric(df["codigo_tempo"], errors="coerce").astype("Int64")
    df["eh_dia"]                = df["eh_dia"].astype("Int64")

    df.dropna(subset=["data_hora", "temperatura_c"], inplace=True)

    df.drop_duplicates(inplace=True)

    return df


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="clima",
        con=engine,
        schema="silver",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando Silver: clima...")

    df = fetch_data()
    df = transform_data(df)
    load_data(df)

    print("Silver clima finalizado!")


if __name__ == "__main__":
    run()
