import pandas as pd
import pytest
from src.transformation_gold.gold_livros import transform_data


# ── Fixture: DataFrame base válido ──────────────────────────────────────────

@pytest.fixture
def df_valido():
    return pd.DataFrame([
        {"key": "/works/OL1", "title": "Livro A", "author_name": "Autor A", "ratings_average": 4.9, "ratings_count": 500,  "first_publish_year": 2000},
        {"key": "/works/OL2", "title": "Livro B", "author_name": "Autor B", "ratings_average": 4.7, "ratings_count": 200,  "first_publish_year": 1990},
        {"key": "/works/OL3", "title": "Livro C", "author_name": "Autor C", "ratings_average": 4.5, "ratings_count": 50,   "first_publish_year": 2010},
        {"key": "/works/OL4", "title": "Livro D", "author_name": "Autor D", "ratings_average": 4.8, "ratings_count": 5,    "first_publish_year": 2015},  # menos de 10 ratings → filtrado
    ])


# ── Testes de filtragem ──────────────────────────────────────────────────────

def test_filtra_livros_com_menos_de_10_ratings(df_valido):
    resultado = transform_data(df_valido)
    assert all(resultado["ratings_count"] >= 10)


def test_livro_com_poucos_ratings_nao_aparece(df_valido):
    resultado = transform_data(df_valido)
    assert "Livro D" not in resultado["title"].values


# ── Testes de ordenação ──────────────────────────────────────────────────────

def test_ordenado_por_rating_decrescente(df_valido):
    resultado = transform_data(df_valido)
    ratings = resultado["ratings_average"].tolist()
    assert ratings == sorted(ratings, reverse=True)


def test_primeiro_lugar_tem_maior_rating(df_valido):
    resultado = transform_data(df_valido)
    assert resultado["title"].iloc[0] == "Livro A"


# ── Testes de estrutura ──────────────────────────────────────────────────────

def test_coluna_ranking_existe(df_valido):
    resultado = transform_data(df_valido)
    assert "ranking" in resultado.columns


def test_ranking_começa_em_1(df_valido):
    resultado = transform_data(df_valido)
    assert resultado["ranking"].iloc[0] == 1


def test_max_10_livros_no_resultado():
    """Com mais de 10 livros elegíveis, deve retornar no máximo 10."""
    df = pd.DataFrame([
        {"key": f"/works/OL{i}", "title": f"Livro {i}", "author_name": f"Autor {i}",
         "ratings_average": round(4.0 + i * 0.01, 2), "ratings_count": 100,
         "first_publish_year": 2000}
        for i in range(15)
    ])
    resultado = transform_data(df)
    assert len(resultado) <= 10


def test_colunas_esperadas(df_valido):
    resultado = transform_data(df_valido)
    for col in ["title", "author_name", "ratings_average", "ratings_count", "first_publish_year", "ranking"]:
        assert col in resultado.columns