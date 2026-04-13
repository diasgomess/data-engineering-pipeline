import requests 
import pandas as pd
from src.database.connection import get_engine

def fetch_data():
    url = (
        "https://openlibrary.org/search.json"
        "?subject=science"
        "&fields=key,title,author_name,first_publish_year,number_of_pages_median,"
        "publisher,language,subject,edition_count,ratings_average,ratings_count,"
        "want_to_read_count,currently_reading_count,already_read_count,isbn"
        "&limit=100"
    )
 
    response = requests.get(url, timeout=30)
 
    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code}")
 
    return response.json()

def transform_data(data):
    docs = data.get("docs", [])
    if not docs:
        raise Exception("Nenhum dado encontrado na resposta da API.")
    
    rows = []
    
    for doc in docs:
        rows.append({
            "key":                    doc.get("key", ""),
            "title":                  doc.get("title", ""),
            "author_name":            ", ".join(doc.get("author_name", [])) if doc.get("author_name") else None,
            "first_publish_year":     doc.get("first_publish_year"),
            "number_of_pages_median": doc.get("number_of_pages_median"),
            "publisher":              ", ".join(doc.get("publisher", [])[:3]) if doc.get("publisher") else None,
            "language":               ", ".join(doc.get("language", [])) if doc.get("language") else None,
            "subject":                ", ".join(doc.get("subject", [])[:5]) if doc.get("subject") else None,
            "edition_count":          doc.get("edition_count"),
            "ratings_average":        doc.get("ratings_average"),
            "ratings_count":          doc.get("ratings_count"),
            "want_to_read_count":     doc.get("want_to_read_count"),
            "currently_reading_count":doc.get("currently_reading_count"),
            "already_read_count":     doc.get("already_read_count"),
            "isbn":                   doc.get("isbn", [None])[0] if doc.get("isbn") else None,
        })

    df = pd.DataFrame(rows)

    int_cols = [
        "first_publish_year", "number_of_pages_median",
        "edition_count", "ratings_count",
        "want_to_read_count", "currently_reading_count", "already_read_count",
    ]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["ratings_average"] = pd.to_numeric(df["ratings_average"], errors="coerce")
 
    return df

def load_data(df):
    engine = get_engine()
 
    df.to_sql(
        name="livros",
        con=engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )

def run():
    print("Iniciando ingestão de livros...")
 
    data = fetch_data()
    df = transform_data(data)
    load_data(df)
 
    print("Ingestão finalizada!")

if __name__ == "__main__":
    run()
        