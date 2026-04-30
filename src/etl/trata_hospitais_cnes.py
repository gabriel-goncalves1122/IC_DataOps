import pandas as pd
import os

# ==========================================
# 1. Configurações de Diretórios e Arquivos
# ==========================================
RAW_DIR = os.path.join('data', 'raw')
AUX_DIR = os.path.join(RAW_DIR, 'cnes_aux')
DIMENSIONS_DIR = os.path.join('data', 'processed', 'dimensions')
os.makedirs(DIMENSIONS_DIR, exist_ok=True)

# Nomes dos arquivos
ARQUIVO_PRINCIPAL = 'tbEstabelecimento202603.csv'
ARQUIVO_MUNICIPIOS = 'tbMunicipio202603.csv'
ARQUIVO_TIPO_UNIDADE = 'tbTipoUnidade202603.csv'

OUTPUT_CSV = os.path.join(DIMENSIONS_DIR, 'dim_hospitais_cnes.csv')

print(f"Lendo base principal: {ARQUIVO_PRINCIPAL} dentro de cnes_aux...")

try:
    # Lendo arquivos com encoding latin-1 (padrão do DATASUS)
    df_estab = pd.read_csv(
        os.path.join(AUX_DIR, ARQUIVO_PRINCIPAL), 
        sep=';', 
        dtype={'CO_MUNICIPIO_GESTOR': str, 'TP_UNIDADE': str, 'CO_CNES': str}, 
        encoding='latin-1'
    )
    
    df_muni = pd.read_csv(
        os.path.join(AUX_DIR, ARQUIVO_MUNICIPIOS), 
        sep=';', 
        dtype=str, 
        encoding='latin-1'
    )
    
    df_tipo = pd.read_csv(
        os.path.join(AUX_DIR, ARQUIVO_TIPO_UNIDADE), 
        sep=';', 
        dtype=str, 
        encoding='latin-1'
    )

except FileNotFoundError as e:
    print("ERRO: Arquivo não encontrado. Verifique se os nomes e pastas estão corretos.")
    print(f"Detalhe: {e}")
    exit(1)

# ==========================================
# 2. Filtragem e Tratamento de Geometria
# ==========================================
colunas_interesse = [
    'CO_CNES', 'NO_FANTASIA', 'CO_MUNICIPIO_GESTOR', 
    'TP_UNIDADE', 'TP_ESTAB_SEMPRE_ABERTO', 'NU_LATITUDE', 'NU_LONGITUDE'
]

colunas_presentes = [col for col in colunas_interesse if col in df_estab.columns]
df_clean = df_estab[colunas_presentes].copy()

# Tratamento de coordenadas
df_clean['NU_LATITUDE'] = pd.to_numeric(df_clean['NU_LATITUDE'].astype(str).str.replace(',', '.'), errors='coerce')
df_clean['NU_LONGITUDE'] = pd.to_numeric(df_clean['NU_LONGITUDE'].astype(str).str.replace(',', '.'), errors='coerce')
df_clean = df_clean.dropna(subset=['NU_LATITUDE', 'NU_LONGITUDE'])

# ==========================================
# 3. Cruzamento Dinâmico (Join com tabelas de domínio)
# ==========================================
print("Cruzando dados com tabelas de municípios e tipos de unidade...")

df_final = pd.merge(
    df_clean, 
    df_muni[['CO_MUNICIPIO', 'NO_MUNICIPIO', 'CO_SIGLA_ESTADO']], 
    left_on='CO_MUNICIPIO_GESTOR', 
    right_on='CO_MUNICIPIO', 
    how='left'
)

df_final = pd.merge(
    df_final, 
    df_tipo[['CO_TIPO_UNIDADE', 'DS_TIPO_UNIDADE']], 
    left_on='TP_UNIDADE', 
    right_on='CO_TIPO_UNIDADE', 
    how='left'
)

# ==========================================
# 4. Tratamento de Dados em Branco
# ==========================================
df_final['NO_FANTASIA'] = df_final['NO_FANTASIA'].fillna('NÃO INFORMADO')
df_final['TP_ESTAB_SEMPRE_ABERTO'] = df_final.get('TP_ESTAB_SEMPRE_ABERTO', pd.Series(['N'] * len(df_final))).fillna('N')
df_final['DS_TIPO_UNIDADE'] = df_final['DS_TIPO_UNIDADE'].fillna('TIPO NÃO INFORMADO')
df_final['NO_MUNICIPIO'] = df_final['NO_MUNICIPIO'].fillna('DESCONHECIDO')
df_final['CO_SIGLA_ESTADO'] = df_final['CO_SIGLA_ESTADO'].fillna('NI')

# ==========================================
# 5. Padronização Final das Colunas
# ==========================================
df_final = df_final.rename(columns={
    'CO_CNES': 'codigo_cnes',
    'NO_FANTASIA': 'nome_hospital',
    'DS_TIPO_UNIDADE': 'tipo_unidade',
    'TP_ESTAB_SEMPRE_ABERTO': 'funciona_24h',
    'CO_MUNICIPIO_GESTOR': 'codigo_ibge_municipio',
    'NO_MUNICIPIO': 'municipios_nome',
    'CO_SIGLA_ESTADO': 'estado_sigla',
    'NU_LATITUDE': 'latitude',
    'NU_LONGITUDE': 'longitude'
})

ordem_final = [
    'codigo_cnes', 'nome_hospital', 'tipo_unidade', 'funciona_24h', 
    'codigo_ibge_municipio', 'municipios_nome', 'estado_sigla', 
    'latitude', 'longitude'
]
df_final = df_final[ordem_final]

# ==========================================
# 6. Exportação
# ==========================================
df_final.to_csv(OUTPUT_CSV, index=False, encoding='utf-8', lineterminator='\n')

print(f"✅ Sucesso! Dimensão de hospitais gerada com {len(df_final)} registros.")
print(f"Arquivo salvo em: {OUTPUT_CSV}")