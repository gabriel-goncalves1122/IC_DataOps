import geopandas as gpd
import pandas as pd
import os

# ==========================================
# 1. Configurações de Diretórios
# ==========================================
RAW_DIR = os.path.join('data', 'raw', 'rodovia-federal') 
DIMENSIONS_DIR = os.path.join('data', 'processed', 'dimensions')
GEO_DIR = os.path.join('data', 'geo')

os.makedirs(DIMENSIONS_DIR, exist_ok=True)
os.makedirs(GEO_DIR, exist_ok=True)

FILE_PATH = os.path.join(RAW_DIR, 'rodovia-federal.shp')
OUTPUT_CSV = os.path.join(DIMENSIONS_DIR, 'dim_rodovias_federais.csv')
OUTPUT_GEOJSON = os.path.join(GEO_DIR, 'mapa_rodovias_federais.geojson')

print(f"Lendo Shapefile da Malha Rodoviária em: {FILE_PATH}")

try:
    gdf = gpd.read_file(FILE_PATH)
except Exception as e:
    print(f"ERRO: Verifique se os arquivos estão em: {RAW_DIR}")
    print(f"Detalhes: {e}")
    exit(1)

# ==========================================
# 2. Tratamento e Extração de Geometria
# ==========================================
# Converte a geometria para WKT (texto) para o banco de dados
gdf['geometria_wkt'] = gdf['geometry'].apply(lambda x: x.wkt)

# LISTA ATUALIZADA COM AS COLUNAS REAIS DO SEU ARQUIVO
colunas_interesse = [
    'ID', 'sigla', 'jurisdicao', 'revestimen', 
    'tipopavime', 'situacaofi', 'operaciona', 
    'geometria_wkt', 'geometry'
]
colunas_presentes = [col for col in colunas_interesse if col in gdf.columns]

gdf_final = gdf[colunas_presentes].copy()

# ==========================================
# 3. Padronização para o Data Warehouse (Druid)
# ==========================================
# Traduzindo as siglas curtas do Shapefile para nomes descritivos
gdf_final = gdf_final.rename(columns={
    'ID': 'id_trecho_rodoviario',
    'sigla': 'codigo_rodovia',
    'jurisdicao': 'jurisdicao',
    'revestimen': 'revestimento',
    'tipopavime': 'tipo_pavimento',
    'situacaofi': 'situacao_fisica',
    'operaciona': 'status_operacional'
})

# ==========================================
# 4. Exportação para os destinos corretos
# ==========================================
print(f"\nSalvando GeoJSON (Mapa) em: {OUTPUT_GEOJSON}")
gdf_final.to_file(OUTPUT_GEOJSON, driver='GeoJSON')

print(f"Salvando CSV (Dimensão) em: {OUTPUT_CSV}")
df_csv = pd.DataFrame(gdf_final.drop(columns='geometry'))
df_csv.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')

print(f"\nProcessamento da malha rodoviária finalizado com sucesso!")