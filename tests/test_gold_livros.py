import pandas as pd
import pytest
from src.transformation_gold.gold_clima import transform_data


# ── Fixture: DataFrame base válido ──────────────────────────────────────────

@pytest.fixture
def df_valido():
    return pd.DataFrame([
        {"data_hora": "2024-01-01T06:00", "temperatura_c": 20.0, "velocidade_vento_kmh": 10.0, "direcao_vento_graus": 180, "codigo_tempo": 1, "eh_dia": 0},
        {"data_hora": "2024-01-01T12:00", "temperatura_c": 30.0, "velocidade_vento_kmh": 20.0, "direcao_vento_graus": 90,  "codigo_tempo": 2, "eh_dia": 1},
        {"data_hora": "2024-01-01T18:00", "temperatura_c": 25.0, "velocidade_vento_kmh": 15.0, "direcao_vento_graus": 45,  "codigo_tempo": 1, "eh_dia": 1},
    ])


# ── Testes de estrutura do output ────────────────────────────────────────────

def test_retorna_uma_linha(df_valido):
    resultado = transform_data(df_valido)
    assert len(resultado) == 1


def test_colunas_esperadas(df_valido):
    resultado = transform_data(df_valido)
    colunas_esperadas = {
        "temperatura_media_c",
        "temperatura_maxima_c",
        "temperatura_minima_c",
        "velocidade_media_vento_kmh",
        "total_registros",
    }
    assert colunas_esperadas.issubset(set(resultado.columns))


# ── Testes de cálculo ────────────────────────────────────────────────────────

def test_temperatura_media_correta(df_valido):
    resultado = transform_data(df_valido)
    assert resultado["temperatura_media_c"].iloc[0] == 25.0


def test_temperatura_maxima_correta(df_valido):
    resultado = transform_data(df_valido)
    assert resultado["temperatura_maxima_c"].iloc[0] == 30.0


def test_temperatura_minima_correta(df_valido):
    resultado = transform_data(df_valido)
    assert resultado["temperatura_minima_c"].iloc[0] == 20.0


def test_total_registros_correto(df_valido):
    resultado = transform_data(df_valido)
    assert resultado["total_registros"].iloc[0] == 3


def test_velocidade_media_vento_correta(df_valido):
    resultado = transform_data(df_valido)
    assert resultado["velocidade_media_vento_kmh"].iloc[0] == 15.0