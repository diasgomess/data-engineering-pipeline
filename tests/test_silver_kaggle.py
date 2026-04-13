import pandas as pd
import pytest
from src.transformation_silver.silver_kaggle import transform_data


# ── Fixture: DataFrame base válido ──────────────────────────────────────────

@pytest.fixture
def df_valido():
    return pd.DataFrame([
        {"CRIM": 0.00632, "ZN": 18.0, "INDUS": 2.31, "CHAS": 0, "NOX": 0.538,
         "RM": 6.575, "AGE": 65.2, "DIS": 4.09, "RAD": 1, "TAX": 296.0,
         "PTRATIO": 15.3, "B": 396.9, "LSTAT": 4.98, "MEDV": 24.0},
        {"CRIM": 0.02731, "ZN": 0.0, "INDUS": 7.07, "CHAS": 0, "NOX": 0.469,
         "RM": 6.421, "AGE": 78.9, "DIS": 4.97, "RAD": 2, "TAX": 242.0,
         "PTRATIO": 17.8, "B": 390.9, "LSTAT": 9.14, "MEDV": 21.6},
    ])


# ── Testes de normalização de colunas ────────────────────────────────────────

def test_colunas_em_minusculo(df_valido):
    resultado = transform_data(df_valido)
    for col in resultado.columns:
        assert col == col.lower(), f"Coluna '{col}' não está em minúsculo"


def test_sem_espacos_nos_nomes_de_colunas(df_valido):
    resultado = transform_data(df_valido)
    for col in resultado.columns:
        assert " " not in col, f"Coluna '{col}' contém espaço"


# ── Testes de limpeza ────────────────────────────────────────────────────────

def test_remove_linhas_completamente_vazias():
    df = pd.DataFrame([
        {"CRIM": None, "ZN": None, "INDUS": None, "CHAS": None, "NOX": None,
         "RM": None, "AGE": None, "DIS": None, "RAD": None, "TAX": None,
         "PTRATIO": None, "B": None, "LSTAT": None, "MEDV": None},
        {"CRIM": 0.00632, "ZN": 18.0, "INDUS": 2.31, "CHAS": 0, "NOX": 0.538,
         "RM": 6.575, "AGE": 65.2, "DIS": 4.09, "RAD": 1, "TAX": 296.0,
         "PTRATIO": 15.3, "B": 396.9, "LSTAT": 4.98, "MEDV": 24.0},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1


def test_remove_duplicatas(df_valido):
    df_duplicado = pd.concat([df_valido, df_valido], ignore_index=True)
    resultado = transform_data(df_duplicado)
    assert len(resultado) == len(df_valido)


# ── Testes de tipos ──────────────────────────────────────────────────────────

def test_colunas_numericas_sao_numericas(df_valido):
    resultado = transform_data(df_valido)
    for col in resultado.columns:
        assert pd.api.types.is_numeric_dtype(resultado[col]), \
            f"Coluna '{col}' deveria ser numérica"


def test_converte_coluna_numerica_em_texto():
    """Garante que colunas numéricas enviadas como texto são convertidas."""
    df = pd.DataFrame([
        {"CRIM": "0.1", "ZN": "18.0", "MEDV": "24.0"},
        {"CRIM": "0.2", "ZN": "20.0", "MEDV": "30.0"},
    ])
    resultado = transform_data(df)
    assert pd.api.types.is_numeric_dtype(resultado["crim"])
    assert pd.api.types.is_numeric_dtype(resultado["medv"])