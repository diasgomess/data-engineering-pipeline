import pandas as pd
import pytest
from src.transformation_silver.silver_clima import transform_data


# ── Fixture: DataFrame base válido ──────────────────────────────────────────

@pytest.fixture
def df_valido():
    return pd.DataFrame([
        {
            "time": "2024-01-01T06:00",
            "temperature": 22.5,
            "windspeed": 10.0,
            "winddirection": 180,
            "weathercode": 1,
            "is_day": 1,
        },
        {
            "time": "2024-01-01T12:00",
            "temperature": 28.0,
            "windspeed": 15.0,
            "winddirection": 90,
            "weathercode": 2,
            "is_day": 1,
        },
    ])


# ── Testes de renomeação de colunas ─────────────────────────────────────────

def test_colunas_renomeadas(df_valido):
    resultado = transform_data(df_valido)
    assert "data_hora" in resultado.columns
    assert "temperatura_c" in resultado.columns
    assert "velocidade_vento_kmh" in resultado.columns
    assert "direcao_vento_graus" in resultado.columns
    assert "codigo_tempo" in resultado.columns
    assert "eh_dia" in resultado.columns


def test_colunas_originais_removidas(df_valido):
    resultado = transform_data(df_valido)
    assert "time" not in resultado.columns
    assert "temperature" not in resultado.columns
    assert "windspeed" not in resultado.columns


# ── Testes de tipos ──────────────────────────────────────────────────────────

def test_data_hora_é_datetime(df_valido):
    resultado = transform_data(df_valido)
    assert pd.api.types.is_datetime64_any_dtype(resultado["data_hora"])


def test_temperatura_é_float(df_valido):
    resultado = transform_data(df_valido)
    assert pd.api.types.is_float_dtype(resultado["temperatura_c"])


def test_codigo_tempo_é_inteiro(df_valido):
    resultado = transform_data(df_valido)
    assert pd.api.types.is_integer_dtype(resultado["codigo_tempo"])


# ── Testes de limpeza ────────────────────────────────────────────────────────

def test_remove_linha_sem_temperatura():
    df = pd.DataFrame([
        {"time": "2024-01-01T06:00", "temperature": None,  "windspeed": 10.0, "winddirection": 180, "weathercode": 1, "is_day": 1},
        {"time": "2024-01-01T12:00", "temperature": 25.0,  "windspeed": 12.0, "winddirection": 90,  "weathercode": 2, "is_day": 1},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1
    assert resultado["temperatura_c"].iloc[0] == 25.0


def test_remove_linha_sem_data_hora():
    df = pd.DataFrame([
        {"time": None,               "temperature": 22.5, "windspeed": 10.0, "winddirection": 180, "weathercode": 1, "is_day": 1},
        {"time": "2024-01-01T12:00", "temperature": 28.0, "windspeed": 15.0, "winddirection": 90,  "weathercode": 2, "is_day": 1},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1


def test_remove_duplicatas():
    df = pd.DataFrame([
        {"time": "2024-01-01T06:00", "temperature": 22.5, "windspeed": 10.0, "winddirection": 180, "weathercode": 1, "is_day": 1},
        {"time": "2024-01-01T06:00", "temperature": 22.5, "windspeed": 10.0, "winddirection": 180, "weathercode": 1, "is_day": 1},
    ])
    resultado = transform_data(df)
    assert len(resultado) == 1


def test_df_vazio_retorna_vazio():
    df = pd.DataFrame(columns=["time", "temperature", "windspeed", "winddirection", "weathercode", "is_day"])
    resultado = transform_data(df)
    assert len(resultado) == 0