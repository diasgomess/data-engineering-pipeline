import pandas as pd
import pytest
from src.transformation_silver.silver_livros import transform_data


# ── Fixture: DataFrame base válido ──────────────────────────────────────────

@pytest.fixture
def df_valido():
    return pd.DataFrame([
        {
            "key": "/works/OL1",
            "title": "A Brief History of Time",
            "author_name": "Stephen Hawking",
            "first_publish_year": 1988,
            "number_of_pages_median": 212,
            "publisher": "Bantam",
            "language": "eng",
            "subject": "Science",
            "edition_count": 50,
            "ratings_average": 4.5,
            "ratings_count": 1000,
            "want_to_read_count": 500,
            "currently_reading_count": 100,
            "already_read_count": 800,
            "isbn": "9780553380163",
        }
    ])


# ── Testes de limpeza ────────────────────────────────────────────────────────

def test_remove_livro_sem_titulo():
    df = pd.DataFrame([
        {"key": "/works/OL1", "title": None,      "author_name": "Autor A", "first_publish_year": 2000, "number_of_pages_median": 200, "publisher": "Ed A", "language": "eng", "subject": "Science", "edition_count": 10, "ratings_average": 4.0, "ratings_count": 100, "want_to_read_count": 50, "currently_reading_count": 10, "already_read_count": 80, "isbn": "111"},
        {"key": "/works/OL2", "title": "Livro B", "author_name": "Autor B", "first_publish_year": 2005, "number_of_pages_median": 300, "publisher": "Ed B", "language": "eng", "subject": "Math",    "edition_count": 5,  "ratings_average": 4.2, "ratings_count": 200, "want_to_read_count": 80, "currently_reading_count": 20, "already_read_count": 150, "isbn": "222"},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1
    assert resultado["title"].iloc[0] == "Livro B"


def test_remove_livro_sem_autor():
    df = pd.DataFrame([
        {"key": "/works/OL1", "title": "Livro A", "author_name": None,     "first_publish_year": 2000, "number_of_pages_median": 200, "publisher": "Ed A", "language": "eng", "subject": "Science", "edition_count": 10, "ratings_average": 4.0, "ratings_count": 100, "want_to_read_count": 50, "currently_reading_count": 10, "already_read_count": 80, "isbn": "111"},
        {"key": "/works/OL2", "title": "Livro B", "author_name": "Autor B", "first_publish_year": 2005, "number_of_pages_median": 300, "publisher": "Ed B", "language": "eng", "subject": "Math",    "edition_count": 5,  "ratings_average": 4.2, "ratings_count": 200, "want_to_read_count": 80, "currently_reading_count": 20, "already_read_count": 150, "isbn": "222"},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1
    assert resultado["author_name"].iloc[0] == "Autor B"


def test_remove_duplicatas_pela_chave(df_valido):
    df_duplicado = pd.concat([df_valido, df_valido], ignore_index=True)
    resultado = transform_data(df_duplicado)
    assert len(resultado) == 1


def test_filtra_ano_invalido_muito_antigo():
    df = pd.DataFrame([
        {"key": "/works/OL1", "title": "Livro A", "author_name": "Autor A", "first_publish_year": 500,  "number_of_pages_median": 200, "publisher": "Ed", "language": "eng", "subject": "S", "edition_count": 1, "ratings_average": 4.0, "ratings_count": 10, "want_to_read_count": 5, "currently_reading_count": 1, "already_read_count": 8, "isbn": "111"},
        {"key": "/works/OL2", "title": "Livro B", "author_name": "Autor B", "first_publish_year": 2000, "number_of_pages_median": 300, "publisher": "Ed", "language": "eng", "subject": "S", "edition_count": 5, "ratings_average": 4.2, "ratings_count": 20, "want_to_read_count": 8, "currently_reading_count": 2, "already_read_count": 15, "isbn": "222"},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1
    assert resultado["first_publish_year"].iloc[0] == 2000


def test_filtra_ano_invalido_futuro():
    df = pd.DataFrame([
        {"key": "/works/OL1", "title": "Livro Futuro", "author_name": "Autor A", "first_publish_year": 2099, "number_of_pages_median": 200, "publisher": "Ed", "language": "eng", "subject": "S", "edition_count": 1, "ratings_average": 4.0, "ratings_count": 10, "want_to_read_count": 5, "currently_reading_count": 1, "already_read_count": 8, "isbn": "111"},
        {"key": "/works/OL2", "title": "Livro B",      "author_name": "Autor B", "first_publish_year": 1990, "number_of_pages_median": 300, "publisher": "Ed", "language": "eng", "subject": "S", "edition_count": 5, "ratings_average": 4.2, "ratings_count": 20, "want_to_read_count": 8, "currently_reading_count": 2, "already_read_count": 15, "isbn": "222"},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1
    assert resultado["title"].iloc[0] == "Livro B"


# ── Testes de tipos ──────────────────────────────────────────────────────────

def test_ratings_average_é_float(df_valido):
    resultado = transform_data(df_valido)
    assert pd.api.types.is_float_dtype(resultado["ratings_average"])


def test_first_publish_year_é_inteiro(df_valido):
    resultado = transform_data(df_valido)
    assert pd.api.types.is_integer_dtype(resultado["first_publish_year"])


# ── Testes de formatação ─────────────────────────────────────────────────────

def test_remove_espacos_do_titulo():
    df = pd.DataFrame([{
        "key": "/works/OL1", "title": "  Livro com espaços  ", "author_name": "Autor A",
        "first_publish_year": 2000, "number_of_pages_median": 200, "publisher": "Ed",
        "language": "eng", "subject": "S", "edition_count": 1, "ratings_average": 4.0,
        "ratings_count": 10, "want_to_read_count": 5, "currently_reading_count": 1,
        "already_read_count": 8, "isbn": "111"
    }])
    resultado = transform_data(df)
    assert resultado["title"].iloc[0] == "Livro com espaços"