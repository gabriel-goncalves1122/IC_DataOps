import math
import pandas as pd
import unicodedata

def limpar_valor_numerico(valor):
    """Converte NaN/Inf para 0.0, protegendo a quebra do JSON."""
    try:
        f_valor = float(valor)
        return 0.0 if math.isnan(f_valor) or math.isinf(f_valor) else f_valor
    except:
        return 0.0

def limpar_texto_relacional(texto):
    """Padroniza chaves de cruzamento (Sem acentos, UPPER, sem quebras invisíveis)."""
    if pd.isna(texto) or not str(texto).strip(): 
        return "NAO INFORMADO"
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').strip().upper()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')