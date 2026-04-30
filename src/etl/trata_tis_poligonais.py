import pandas as pd
import geopandas as gpd
from shapely import wkt
import unicodedata
import os
import warnings

# Ignora avisos internos de geometria do Pandas
warnings.filterwarnings("ignore", category=UserWarning)

# ==========================================
# 1. Configurações de Diretórios
# ==========================================
RAW_DIR = os.path.join('data', 'raw')
DIMENSIONS_DIR = os.path.join('data', 'processed', 'dimensions')
GEO_DIR = os.path.join('data', 'geo')

os.makedirs(DIMENSIONS_DIR, exist_ok=True)
os.makedirs(GEO_DIR, exist_ok=True)

FILE_PATH = os.path.join(RAW_DIR, 'tis_poligonais.csv')
OUTPUT_CSV = os.path.join(DIMENSIONS_DIR, 'dim_terras_indigenas_funai.csv')
OUTPUT_GEOJSON = os.path.join(GEO_DIR, 'mapa_terras_indigenas.geojson')

def remover_acentos_e_sujeira(texto):
    """A Chave de Ouro: padroniza nomes para o Join com MapBiomas e Focos"""
    if pd.isna(texto) or not texto: return "NAO INFORMADO"
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').strip().upper()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

print("==================================================")
print("🪶 ETL: DIMENSÃO DE TERRAS INDÍGENAS (FUNAI)")
print("==================================================")

print(f"Lendo base oficial da FUNAI em: {FILE_PATH}")
print("Isso pode levar alguns segundos...")

try:
    df = pd.read_csv(FILE_PATH)
except FileNotFoundError:
    print(f"❌ ERRO: Arquivo não encontrado. Coloque 'tis_poligonais.csv' na pasta {RAW_DIR}")
    exit(1)

# ==========================================
# 2. Limpeza e Seleção de Colunas
# ==========================================
print("Aplicando limpeza de chaves para compatibilidade relacional...")

colunas_interesse = [
    'terrai_codigo', 'terrai_nome', 'etnia_nome', 'municipio_nome',
    'uf_sigla', 'superficie_perimetro_ha', 'fase_ti', 'modalidade_ti',
    'faixa_fronteira', 'the_geom'
]

colunas_presentes = [col for col in colunas_interesse if col in df.columns]
df_clean = df[colunas_presentes].copy()

# ==========================================
# 3. Padronização para o Data Warehouse (Druid)
# ==========================================
# Aplica a limpeza OBRIGATÓRIA no nome da TI para o JOIN funcionar com o MapBiomas
df_clean['terrai_nome'] = df_clean['terrai_nome'].apply(remover_acentos_e_sujeira)

df_clean = df_clean.rename(columns={
    'terrai_codigo': 'codigo_ti',
    'terrai_nome': 'nome_terra_indigena',
    'etnia_nome': 'etnia',
    'municipio_nome': 'municipios',
    'uf_sigla': 'estado',
    'superficie_perimetro_ha': 'area_hectares',
    'fase_ti': 'estagio_regularizacao',
    'modalidade_ti': 'modalidade',
    'faixa_fronteira': 'em_faixa_fronteira',
    'the_geom': 'geometria_wkt'
})

# ==========================================
# 4. Exportação do CSV Analítico (Druid)
# ==========================================
print(f"Salvando CSV (Dimensão) em: {OUTPUT_CSV}")
# lineterminator='\n' blinda o CSV contra falhas de parse no Druid
df_clean.to_csv(OUTPUT_CSV, index=False, encoding='utf-8', lineterminator='\n')

# ==========================================
# 5. Transformação Espacial para o Superset (GeoJSON)
# ==========================================
print("Convertendo WKT para geometria espacial (isso exige um pouco de processamento)...")

# Converte o texto WKT em polígonos matemáticos usando o Shapely
df_clean['geometry'] = df_clean['geometria_wkt'].apply(wkt.loads)

# Removemos a coluna de texto longa para deixar o GeoJSON mais leve
df_clean = df_clean.drop(columns=['geometria_wkt'])

# Criamos o GeoDataFrame
gdf = gpd.GeoDataFrame(df_clean, geometry='geometry')

# Os dados da FUNAI geralmente vêm no sistema brasileiro SIRGAS 2000 (EPSG:4674)
gdf.set_crs(epsg=4674, allow_override=True, inplace=True)

# Convertemos para WGS84 (EPSG:4326), linguagem universal de mapas web
gdf = gdf.to_crs(epsg=4326)

print(f"Salvando GeoJSON (Mapa base) em: {OUTPUT_GEOJSON}")
gdf.to_file(OUTPUT_GEOJSON, driver='GeoJSON')

print("\n✅ Processamento da base da FUNAI concluído com excelência!")