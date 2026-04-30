import geopandas as gpd
import pandas as pd
import unicodedata
import os
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

# ==========================================
# 1. Configurações
# ==========================================
GEO_DIR = os.path.join('data', 'geo')
DIMENSIONS_DIR = os.path.join('data', 'processed', 'dimensions')
os.makedirs(DIMENSIONS_DIR, exist_ok=True)

INPUT_GEOJSON = os.path.join(GEO_DIR, 'sistemas_isolados_ons.geojson') 
OUTPUT_CSV = os.path.join(DIMENSIONS_DIR, 'dim_ons_sistemas_isolados.csv')

def remover_acentos(texto):
    """Limpeza padrão DataOps para garantir indexação limpa."""
    if pd.isna(texto) or not texto: return "NAO INFORMADO"
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').strip().upper()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

print("==================================================")
print("🔋 ETL: INFRAESTRUTURA CRÍTICA (SISTEMAS ISOLADOS)")
print("==================================================")

print("1. Lendo GeoJSON de Sistemas Isolados...")
try:
    gdf = gpd.read_file(INPUT_GEOJSON)
except FileNotFoundError:
    print(f"❌ ERRO: Arquivo {INPUT_GEOJSON} não encontrado.")
    exit(1)

# ==========================================
# 2. Extração e Limpeza de Dados
# ==========================================
print("2. Limpando dados e extraindo metadados...")

# O nome vem limpo na propriedade 'Name'
gdf['nome_sistema_isolado'] = gdf['Name'].apply(remover_acentos)

# A coluna PopupInfo traz a sigla do estado separada por hífen (ex: "Afuá - PA")
# Vamos extrair apenas a sigla do estado.
def extrair_estado(popup_info):
    if pd.isna(popup_info): return "NA"
    partes = str(popup_info).split('-')
    if len(partes) > 1:
        return partes[-1].strip().upper()
    return "NA"

gdf['estado_sigla'] = gdf['PopupInfo'].apply(extrair_estado)

# ==========================================
# 3. Geometria e Coordenadas
# ==========================================
print("3. Extraindo coordenadas e WKT...")

if gdf.crs is None:
    gdf.set_crs(epsg=4326, inplace=True)

# Extrai Latitude e Longitude dos Pontos
gdf['latitude'] = gdf.geometry.y.round(6)
gdf['longitude'] = gdf.geometry.x.round(6)

# Converte o Ponto para WKT para padronizar o pipeline de renderização do Superset
gdf['geometria_wkt'] = gdf.geometry.to_wkt()

# ==========================================
# 4. Organização Final e Exportação
# ==========================================
print("4. Organizando arquivo final...")

df_final = pd.DataFrame(gdf)

# Criamos um ID oficial
df_final['id_sistema_isolado'] = df_final.index + 1

# Selecionamos apenas as colunas vitais (descartando o lixo do KML original)
colunas_finais = [
    'id_sistema_isolado', 
    'nome_sistema_isolado', 
    'estado_sigla', 
    'latitude', 
    'longitude', 
    'geometria_wkt'
]

df_final = df_final[[c for c in colunas_finais if c in df_final.columns]]

# Exporta garantindo o padrão Linux (lineterminator=\n)
df_final.to_csv(OUTPUT_CSV, index=False, encoding='utf-8', lineterminator='\n')

print(f"✅ SUCESSO! {len(df_final)} sistemas isolados salvos em: {OUTPUT_CSV}")