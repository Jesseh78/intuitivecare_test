"""
Testes unitários para agregação de despesas por operadora/UF
"""
import pytest
import pandas as pd
import numpy as np


def aggregate_operadora_uf(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa despesas por RazaoSocial e UF.
    Calcula: TotalDespesas, MediaDespesasTrimestre, DesvioPadraoDespesas
    """
    df = df.copy()
    df["ValorDespesas"] = pd.to_numeric(df["ValorDespesas"], errors="coerce")
    
    group_cols = ["RazaoSocial", "UF"]
    
    agg = (
        df.groupby(group_cols, dropna=False)["ValorDespesas"]
        .agg(
            TotalDespesas="sum",
            MediaDespesasTrimestre="mean",
            DesvioPadraoDespesas="std",
        )
        .reset_index()
    )
    
    # Arredonda para 2 casas decimais
    agg["TotalDespesas"] = agg["TotalDespesas"].round(2)
    agg["MediaDespesasTrimestre"] = agg["MediaDespesasTrimestre"].round(2)
    agg["DesvioPadraoDespesas"] = agg["DesvioPadraoDespesas"].round(2)
    
    return agg


# ============================================
# Testes de agregação
# ============================================

def test_aggregate_simples_uma_operadora():
    """Testa agregação com uma operadora em um estado"""
    data = pd.DataFrame({
        "RazaoSocial": ["UNIMED ABC", "UNIMED ABC", "UNIMED ABC"],
        "UF": ["SP", "SP", "SP"],
        "ValorDespesas": [1000.0, 2000.0, 3000.0],
    })
    
    result = aggregate_operadora_uf(data)
    
    assert len(result) == 1
    assert result.iloc[0]["RazaoSocial"] == "UNIMED ABC"
    assert result.iloc[0]["UF"] == "SP"
    assert result.iloc[0]["TotalDespesas"] == 6000.0
    assert result.iloc[0]["MediaDespesasTrimestre"] == 2000.0


def test_aggregate_multiplas_operadoras():
    """Testa agregação com múltiplas operadoras"""
    data = pd.DataFrame({
        "RazaoSocial": ["UNIMED ABC", "UNIMED ABC", "BRADESCO SAUDE", "BRADESCO SAUDE"],
        "UF": ["SP", "SP", "RJ", "RJ"],
        "ValorDespesas": [1000.0, 2000.0, 5000.0, 5000.0],
    })
    
    result = aggregate_operadora_uf(data)
    
    assert len(result) == 2
    
    # UNIMED
    unimed = result[result["RazaoSocial"] == "UNIMED ABC"].iloc[0]
    assert unimed["TotalDespesas"] == 3000.0
    assert unimed["MediaDespesasTrimestre"] == 1500.0
    
    # BRADESCO
    bradesco = result[result["RazaoSocial"] == "BRADESCO SAUDE"].iloc[0]
    assert bradesco["TotalDespesas"] == 10000.0
    assert bradesco["MediaDespesasTrimestre"] == 5000.0


def test_aggregate_mesma_operadora_estados_diferentes():
    """Testa agregação com mesma operadora em UFs diferentes"""
    data = pd.DataFrame({
        "RazaoSocial": ["UNIMED ABC", "UNIMED ABC", "UNIMED ABC"],
        "UF": ["SP", "RJ", "SP"],
        "ValorDespesas": [1000.0, 2000.0, 3000.0],
    })
    
    result = aggregate_operadora_uf(data)
    
    # Deve gerar 2 linhas: UNIMED/SP e UNIMED/RJ
    assert len(result) == 2
    
    sp = result[(result["RazaoSocial"] == "UNIMED ABC") & (result["UF"] == "SP")].iloc[0]
    assert sp["TotalDespesas"] == 4000.0  # 1000 + 3000
    assert sp["MediaDespesasTrimestre"] == 2000.0
    
    rj = result[(result["RazaoSocial"] == "UNIMED ABC") & (result["UF"] == "RJ")].iloc[0]
    assert rj["TotalDespesas"] == 2000.0


def test_aggregate_desvio_padrao():
    """Testa cálculo do desvio padrão"""
    data = pd.DataFrame({
        "RazaoSocial": ["UNIMED ABC"] * 4,
        "UF": ["SP"] * 4,
        "ValorDespesas": [100.0, 200.0, 300.0, 400.0],  # std = 129.09
    })
    
    result = aggregate_operadora_uf(data)
    
    assert len(result) == 1
    # Desvio padrão amostral de [100, 200, 300, 400]
    expected_std = np.std([100, 200, 300, 400], ddof=1)
    assert abs(result.iloc[0]["DesvioPadraoDespesas"] - round(expected_std, 2)) < 0.01


def test_aggregate_valores_zero():
    """Testa agregação com valores zero"""
    data = pd.DataFrame({
        "RazaoSocial": ["UNIMED ABC", "UNIMED ABC"],
        "UF": ["SP", "SP"],
        "ValorDespesas": [0.0, 0.0],
    })
    
    result = aggregate_operadora_uf(data)
    
    assert len(result) == 1
    assert result.iloc[0]["TotalDespesas"] == 0.0
    assert result.iloc[0]["MediaDespesasTrimestre"] == 0.0


def test_aggregate_valores_string_converte_para_float():
    """Testa que valores string são convertidos para float"""
    data = pd.DataFrame({
        "RazaoSocial": ["UNIMED ABC", "UNIMED ABC"],
        "UF": ["SP", "SP"],
        "ValorDespesas": ["1000.50", "2000.75"],
    })
    
    result = aggregate_operadora_uf(data)
    
    assert len(result) == 1
    assert result.iloc[0]["TotalDespesas"] == 3001.25


def test_aggregate_valores_invalidos_ignorados():
    """Testa que valores inválidos são tratados como NaN e somados como 0"""
    data = pd.DataFrame({
        "RazaoSocial": ["UNIMED ABC", "UNIMED ABC", "UNIMED ABC"],
        "UF": ["SP", "SP", "SP"],
        "ValorDespesas": [1000.0, "abc", 2000.0],  # "abc" vira NaN
    })
    
    result = aggregate_operadora_uf(data)
    
    assert len(result) == 1
    # NaN é ignorado na soma
    assert result.iloc[0]["TotalDespesas"] == 3000.0
