import pandas as pd
import os
import unicodedata

# ==========================================
# 1. Configurações de Diretórios e Arquivos
# ==========================================
NOME_ARQUIVO_EXCEL = 'MAPBIOMAS_BRAZIL-COVERAGE_STATISTICS-COL.10.1-MUNICIPALITIES_STATES_BIOMES.xlsx'
NOME_ABA_COBERTURA = 'COVERAGE_10.1'
NOME_COLUNA_TERRITORIO = 'municipality' 
NOME_ARQUIVO_SAIDA = 'dim_mapbiomas_municipios.csv'

RAW_DIR = os.path.join('data', 'raw')
PROCESSED_DIR = os.path.join('data', 'processed', 'dimensions')
os.makedirs(PROCESSED_DIR, exist_ok=True)

FILE_PATH = os.path.join(RAW_DIR, NOME_ARQUIVO_EXCEL)
OUTPUT_CSV = os.path.join(PROCESSED_DIR, NOME_ARQUIVO_SAIDA)

def remover_acentos(texto):
    """Padroniza para MAIÚSCULO e sem acentos para garantir o Join."""
    if pd.isna(texto): return ""
    texto = str(texto).upper().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

print(f"Lendo base de Municípios MapBiomas: {NOME_ARQUIVO_EXCEL}")

try:
    df_coverage = pd.read_excel(FILE_PATH, sheet_name=NOME_ABA_COBERTURA)
    df_legend = pd.read_excel(FILE_PATH, sheet_name='LEGEND_CODE', skiprows=3)
except Exception as e:
    print(f"ERRO ao ler Excel: {e}")
    exit(1)

# ==========================================
# 2. Identificando o Ano Mais Recente
# ==========================================
colunas_anos = [col for col in df_coverage.columns if str(col).isdigit() and len(str(col)) == 4]
ano_mais_recente = max(colunas_anos)

# ==========================================
# 3. Tratamento e Normalização (DATAOPS CLEANING)
# ==========================================
if NOME_COLUNA_TERRITORIO not in df_coverage.columns:
    print(f"ERRO: Coluna {NOME_COLUNA_TERRITORIO} não encontrada.")
    exit(1)

colunas_identificacao = [NOME_COLUNA_TERRITORIO, 'state', 'biome', 'class_id']
df_recent = df_coverage[colunas_identificacao + [ano_mais_recente]].copy()

# LIMPEZA CRÍTICA PARA O JOIN COM FOCOS DE INCÊNDIO
df_recent['municipio_norm'] = df_recent[NOME_COLUNA_TERRITORIO].apply(remover_acentos)
df_recent['estado_norm'] = df_recent['state'].apply(remover_acentos)
df_recent['bioma_norm'] = df_recent['biome'].apply(remover_acentos)

# CRIAÇÃO DA CHAVE MESTRA (Ex: MATO GROSSO_VILA BELA DA SANTISSIMA TRINDADE)
df_recent['chave_localidade'] = df_recent['estado_norm'] + "_" + df_recent['municipio_norm']

df_recent = df_recent.rename(columns={ano_mais_recente: 'area_hectares'})
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
# 5. Organização Final
# ==========================================
df_final = df_final.rename(columns={'class_id': 'id_classe_solo'})

# Ordem das colunas otimizada para o Druid
ordem_colunas = [
    'chave_localidade', 'municipio_norm', 'estado_norm', 'bioma_norm', 
    'id_classe_solo', 'categoria_macro', 'uso_cobertura_solo', 
    'area_hectares'
]
df_final = df_final[ordem_colunas].fillna("NAO INFORMADO")

# ==========================================
# 6. Exportação
# ==========================================
df_final.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')

print(f"Sucesso! Dimensão Municípios gerada com chave_localidade para {len(df_final)} registros.")