"""
Testes unitários para validação e normalização de CNPJ
"""
import pytest


def is_valid_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ usando algoritmo de dígitos verificadores.
    Espera string com 14 dígitos numéricos.
    """
    if not cnpj or len(cnpj) != 14 or not cnpj.isdigit():
        return False

    # CNPJ com todos dígitos iguais é inválido
    if cnpj == cnpj[0] * 14:
        return False

    # Calcula primeiro dígito verificador
    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum1 = sum(int(cnpj[i]) * weights1[i] for i in range(12))
    digit1 = 0 if (sum1 % 11) < 2 else (11 - (sum1 % 11))

    # Calcula segundo dígito verificador
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum2 = sum(int(cnpj[i]) * weights2[i] for i in range(13))
    digit2 = 0 if (sum2 % 11) < 2 else (11 - (sum2 % 11))

    return cnpj[-2:] == f"{digit1}{digit2}"


def normalize_cnpj(cnpj: str) -> str:
    """
    Remove formatação do CNPJ e retorna apenas dígitos.
    """
    if not cnpj:
        return ""
    return "".join(c for c in str(cnpj) if c.isdigit())


# ============================================
# Testes de validação de CNPJ
# ============================================

def test_cnpj_valido_formato_completo():
    """Testa CNPJ válido com 14 dígitos"""
    # CNPJ válido real (formato já normalizado)
    assert is_valid_cnpj("00000000000191") is True
    assert is_valid_cnpj("11222333000181") is True


def test_cnpj_invalido_digito_verificador_errado():
    """Testa CNPJ com dígito verificador incorreto"""
    assert is_valid_cnpj("11222333000199") is False  # DV errado
    assert is_valid_cnpj("00000000000100") is False  # DV errado


def test_cnpj_invalido_todos_digitos_iguais():
    """Testa CNPJ com todos dígitos iguais (inválido)"""
    assert is_valid_cnpj("00000000000000") is False
    assert is_valid_cnpj("11111111111111") is False


def test_cnpj_invalido_tamanho_incorreto():
    """Testa CNPJ com tamanho diferente de 14 dígitos"""
    assert is_valid_cnpj("123") is False
    assert is_valid_cnpj("123456789012345") is False  # 15 dígitos


def test_cnpj_invalido_vazio_ou_none():
    """Testa CNPJ vazio ou None"""
    assert is_valid_cnpj("") is False
    assert is_valid_cnpj(None) is False


def test_cnpj_invalido_com_letras():
    """Testa CNPJ com caracteres não numéricos"""
    assert is_valid_cnpj("1122233300018A") is False
    assert is_valid_cnpj("ABC12333000181") is False


# ============================================
# Testes de normalização de CNPJ
# ============================================

def test_normalize_cnpj_com_formatacao():
    """Testa normalização de CNPJ formatado"""
    assert normalize_cnpj("11.222.333/0001-81") == "11222333000181"
    assert normalize_cnpj("00.000.000/0001-91") == "00000000000191"


def test_normalize_cnpj_apenas_numeros():
    """Testa normalização de CNPJ já sem formatação"""
    assert normalize_cnpj("11222333000181") == "11222333000181"


def test_normalize_cnpj_vazio():
    """Testa normalização de CNPJ vazio"""
    assert normalize_cnpj("") == ""
    assert normalize_cnpj(None) == ""


def test_normalize_cnpj_com_espacos():
    """Testa normalização de CNPJ com espaços"""
    assert normalize_cnpj("11 222 333 0001 81") == "11222333000181"
    assert normalize_cnpj("  11.222.333/0001-81  ") == "11222333000181"
