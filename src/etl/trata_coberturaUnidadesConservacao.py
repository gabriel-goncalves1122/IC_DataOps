import pandas as pd
import os
import unicodedata

# ==========================================
# 1. Configurações de Diretórios e Arquivos
# ==========================================
NOME_ARQUIVO_EXCEL = 'MAPBIOMAS_BRAZIL-COVERAGE_STATISTICS-COL.10.1-PROTECTED_AREAS_STATE_BIOME.xlsx'
NOME_ABA_COBERTURA = 'COVERAGE_PROTECTED_AREAS'
NOME_ARQUIVO_SAIDA = 'dim_mapbiomas_unidades_conservacao.csv'

RAW_DIR = os.path.join('data', 'raw')
PROCESSED_DIR = os.path.join('data', 'processed', 'dimensions')
os.makedirs(PROCESSED_DIR, exist_ok=True)

FILE_PATH = os.path.join(RAW_DIR, NOME_ARQUIVO_EXCEL)
OUTPUT_CSV = os.path.join(PROCESSED_DIR, NOME_ARQUIVO_SAIDA)

def remover_acentos(texto):
    """Padroniza para MAIÚSCULO e sem acentos."""
    if pd.isna(texto): return ""
    texto = str(texto).upper().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def limpar_nome_uc(texto):
    """Remove códigos e IDs entre parênteses. Ex: 'RESEX DO CAZUMBA-IRACEMA (0000...)' -> 'RESEX DO CAZUMBA-IRACEMA'"""
    if pd.isna(texto): return ""
    nome_puro = str(texto).split('(')[0].strip()
    return remover_acentos(nome_puro)

print(f"Lendo base de Unidades de Conservação: {NOME_ARQUIVO_EXCEL}")

try:
    df_coverage = pd.read_excel(FILE_PATH, sheet_name=NOME_ABA_COBERTURA)
    df_legend = pd.read_excel(FILE_PATH, sheet_name='LEGEND_CODE', skiprows=3)
except Exception as e:
    print(f"ERRO: {e}")
    exit(1)

# ==========================================
# 2. Identificando o Ano Mais Recente
# ==========================================
colunas_anos = [col for col in df_coverage.columns if str(col).isdigit() and len(str(col)) == 4]
ano_mais_recente = max(colunas_anos)

# ==========================================
# 3. Tratamento e Normalização (DATAOPS CLEANING)
# ==========================================
colunas_identificacao = [
    'protected_area_name', 'state', 'biome', 
    'protected_area_political_level', 'protected_area_use_type', 
    'protected_area_type', 'class' 
]

df_recent = df_coverage[colunas_identificacao + [ano_mais_recente]].copy()

# APLICANDO A LIMPEZA PESADA
df_recent['nome_uc_limpo'] = df_recent['protected_area_name'].apply(limpar_nome_uc)
df_recent['estado_norm'] = df_recent['state'].apply(remover_acentos)
df_recent['bioma_norm'] = df_recent['biome'].apply(remover_acentos)

df_recent = df_recent.rename(columns={ano_mais_recente: 'area_hectares', 'class': 'class_id'})
df_recent = df_recent[df_recent['area_hectares'] > 0]
df_recent['class_id'] = pd.to_numeric(df_recent['class_id'], errors='coerce')

# ==========================================
# 4. Tratamento da Legenda
# ==========================================
df_legend_clean = df_legend[['Code ID', 'COLEÇÃO 10 - CLASSES']].copy()
df_legend_clean.columns = ['class_id', 'uso_cobertura_solo']
df_legend_clean = df_legend_clean.dropna()

def get_macro(text):
    text = str(text)
    if text.startswith('1.'): return '1. Floresta'
    elif text.startswith('2.'): return '2. Vegetação Não Florestal'
    elif text.startswith('3.'): return '3. Agropecuária'
    elif text.startswith('4.'): return '4. Área não Vegetada'
    elif text.startswith('5.'): return '5. Corpo D\'água'
    else: return 'Outros'

df_legend_clean['categoria_macro'] = df_legend_clean['uso_cobertura_solo'].apply(get_macro)

# Merge
df_final = pd.merge(df_recent, df_legend_clean, on='class_id', how='left')

# ==========================================
# 5. Organização Final (Data Quality)
# ==========================================
df_final = df_final.rename(columns={
    'protected_area_political_level': 'esfera_governamental',
    'protected_area_use_type': 'tipo_uso',
    'protected_area_type': 'categoria_uc',
    'class_id': 'id_classe_solo'
})

ordem_colunas = [
    'nome_uc_limpo', 'esfera_governamental', 'tipo_uso', 'categoria_uc', 
    'estado_norm', 'bioma_norm', 'id_classe_solo', 'categoria_macro', 
    'uso_cobertura_solo', 'area_hectares'
]
df_final = df_final[ordem_colunas].fillna("NAO INFORMADO")

# ==========================================
# 6. Exportação para CSV
# ==========================================
df_final.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')

print(f"Sucesso! Dimensão UCs gerada com nomes limpos para {len(df_final)} registros.")