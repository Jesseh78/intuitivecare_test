"""
Testes unitários para parsing de valores monetários (brasileiro vs internacional)
"""
import pytest
from decimal import Decimal


def to_decimal_br(value: str) -> float:
    """
    Converte string com valor monetário para float.
    Suporta formato brasileiro (vírgula decimal) e internacional (ponto decimal).
    
    Exemplos:
        "1.234,56" → 1234.56
        "1234.56" → 1234.56
        "1,234.56" → 1234.56 (formato US)
    """
    if not value or not isinstance(value, str):
        return 0.0
    
    value = value.strip().replace(" ", "")
    
    # Remove símbolos de moeda
    value = value.replace("R$", "").replace("$", "").strip()
    
    # Conta pontos e vírgulas para determinar o formato
    num_pontos = value.count(".")
    num_virgulas = value.count(",")
    
    # Formato brasileiro: "1.234,56" ou "1234,56"
    if num_virgulas > 0 and (num_pontos == 0 or value.rfind(",") > value.rfind(".")):
        value = value.replace(".", "").replace(",", ".")
    # Formato internacional com separador de milhar: "1,234.56"
    elif num_virgulas > 0 and num_pontos > 0:
        value = value.replace(",", "")
    # Apenas vírgula sem ponto: assume decimal brasileiro
    elif num_virgulas == 1 and num_pontos == 0:
        value = value.replace(",", ".")
    
    try:
        return float(value)
    except (ValueError, AttributeError):
        return 0.0


# ============================================
# Testes de parsing de valores
# ============================================

def test_parse_valor_formato_brasileiro_com_separador_milhar():
    """Testa formato brasileiro: 1.234,56"""
    assert to_decimal_br("1.234,56") == 1234.56
    assert to_decimal_br("10.000,00") == 10000.00
    assert to_decimal_br("1.234.567,89") == 1234567.89


def test_parse_valor_formato_brasileiro_sem_separador_milhar():
    """Testa formato brasileiro sem milhar: 1234,56"""
    assert to_decimal_br("1234,56") == 1234.56
    assert to_decimal_br("500,00") == 500.00


def test_parse_valor_formato_internacional():
    """Testa formato internacional (US): 1,234.56"""
    assert to_decimal_br("1,234.56") == 1234.56
    assert to_decimal_br("10,000.00") == 10000.00
    assert to_decimal_br("1,234,567.89") == 1234567.89


def test_parse_valor_apenas_ponto_decimal():
    """Testa formato apenas com ponto: 1234.56"""
    assert to_decimal_br("1234.56") == 1234.56
    assert to_decimal_br("500.00") == 500.00


def test_parse_valor_inteiro_sem_decimais():
    """Testa valores inteiros"""
    assert to_decimal_br("1234") == 1234.0
    assert to_decimal_br("1000") == 1000.0


def test_parse_valor_com_simbolo_moeda():
    """Testa valores com símbolos R$ ou $"""
    assert to_decimal_br("R$ 1.234,56") == 1234.56
    assert to_decimal_br("$ 1,234.56") == 1234.56
    assert to_decimal_br("R$1234,56") == 1234.56


def test_parse_valor_com_espacos():
    """Testa valores com espaços extras"""
    assert to_decimal_br("  1.234,56  ") == 1234.56
    assert to_decimal_br("1 234,56") == 1234.56


def test_parse_valor_invalido():
    """Testa valores inválidos (retorna 0.0)"""
    assert to_decimal_br("") == 0.0
    assert to_decimal_br(None) == 0.0
    assert to_decimal_br("abc") == 0.0
    assert to_decimal_br("R$") == 0.0


def test_parse_valor_zero():
    """Testa valor zero"""
    assert to_decimal_br("0") == 0.0
    assert to_decimal_br("0,00") == 0.0
    assert to_decimal_br("0.00") == 0.0
