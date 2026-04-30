import pandas as pd
import os
import unicodedata

# ==========================================
# 1. Configurações de Diretórios e Arquivos
# ==========================================
RAW_DIR = os.path.join('data', 'raw')
PROCESSED_DIR = os.path.join('data', 'processed', 'dimensions')
os.makedirs(PROCESSED_DIR, exist_ok=True)

FILE_PATH = os.path.join(RAW_DIR, 'MAPBIOMAS_BRAZIL-COVERAGE_STATISTICS-COL.10.1-INDIGENOUS_TERRITORIES_STATE_BIOME.xlsx')
OUTPUT_CSV = os.path.join(PROCESSED_DIR, 'dim_mapbiomas_terras_indigenas.csv')

def remover_acentos(texto):
    if pd.isna(texto): return ""
    texto = str(texto).upper().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def limpar_nome_mapbiomas(texto):
    """Remove o código entre parênteses. Ex: 'Alto Rio Purus (1201)' -> 'ALTO RIO PURUS'"""
    if pd.isna(texto): return ""
    nome_puro = str(texto).split('(')[0].strip()
    return remover_acentos(nome_puro)

print("Lendo abas do arquivo Excel MapBiomas (Terras Indígenas)...")

try:
    df_coverage = pd.read_excel(FILE_PATH, sheet_name='COVERAGE_INDIGENOUS_TERRITORIES')
    df_legend = pd.read_excel(FILE_PATH, sheet_name='LEGEND_CODE', skiprows=3)
except FileNotFoundError:
    print(f"ERRO: Arquivo não encontrado em {FILE_PATH}.")
    exit(1)

# ==========================================
# 2. Identificando o Ano Mais Recente
# ==========================================
colunas_anos = [col for col in df_coverage.columns if str(col).isdigit() and len(str(col)) == 4]
ano_mais_recente = max(colunas_anos)

# ==========================================
# 3. Tratamento e Normalização (Data Quality)
# ==========================================
colunas_identificacao = ['indigenous_territories', 'state', 'biome', 'class_id']
df_recent = df_coverage[colunas_identificacao + [ano_mais_recente]].copy()

# APLICANDO A LIMPEZA PESADA
df_recent['nome_ti_limpo'] = df_recent['indigenous_territories'].apply(limpar_nome_mapbiomas)
df_recent['estado_norm'] = df_recent['state'].apply(remover_acentos)
df_recent['bioma_norm'] = df_recent['biome'].apply(remover_acentos)

df_recent = df_recent.rename(columns={ano_mais_recente: 'area_hectares'})
df_recent = df_recent[df_recent['area_hectares'] > 0]
df_recent['class_id'] = pd.to_numeric(df_recent['class_id'], errors='coerce')

# ==========================================
# 4. Tratamento da Legenda
# ==========================================
df_legend_clean = df_legend[['Code ID', 'COLEÇÃO 10 - CLASSES']].copy()
df_legend_clean.columns = ['class_id', 'uso_cobertura_solo']
df_legend_clean = df_legend_clean.dropna()
df_legend_clean['class_id'] = pd.to_numeric(df_legend_clean['class_id'], errors='coerce')

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

# Reordenando com as colunas limpas
ordem_colunas = [
    'nome_ti_limpo', 'estado_norm', 'bioma_norm', 
    'id_classe_solo', 'categoria_macro', 'uso_cobertura_solo', 
    'area_hectares'
]
df_final = df_final[ordem_colunas].fillna("NAO INFORMADO")

# ==========================================
# 6. Exportação
# ==========================================
df_final.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')

print(f"Sucesso! Dimensão Terras Indígenas (MapBiomas) limpa e normalizada.")
print(f"Arquivo: {OUTPUT_CSV}")