import requests
import pandas as pd
from src.database.connection import get_engine


def fetch_data():
    url = "https://api.open-meteo.com/v1/forecast?latitude=-23.55&longitude=-46.63&current_weather=true"
    
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code}")

    return response.json()


def transform_data(data):
    weather = data.get("current_weather", {})
    df = pd.DataFrame([weather])
    return df


def load_data(df):
    engine = get_engine()

    df.to_sql(
        name="clima",
        con=engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )


def run():
    print("Iniciando ingestão de clima...")

    data = fetch_data()
    df = transform_data(data)
    load_data(df)

    print("Ingestão finalizada!")


if __name__ == "__main__":
    run()