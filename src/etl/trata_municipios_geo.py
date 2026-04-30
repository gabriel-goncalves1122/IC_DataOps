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

INPUT_GEOJSON = os.path.join(GEO_DIR, 'municipios_br.geojson') 
OUTPUT_CSV = os.path.join(DIMENSIONS_DIR, 'dim_municipios_coordenadas.csv')

def remover_acentos_e_sujeira(texto):
    if pd.isna(texto) or not texto: return ""
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').strip().upper()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

mapa_estados = {
    '11': 'RONDONIA', '12': 'ACRE', '13': 'AMAZONAS', '14': 'RORAIMA', '15': 'PARA', 
    '16': 'AMAPA', '17': 'TOCANTINS', '21': 'MARANHAO', '22': 'PIAUI', '23': 'CEARA', 
    '24': 'RIO GRANDE DO NORTE', '25': 'PARAIBA', '26': 'PERNAMBUCO', '27': 'ALAGOAS', 
    '28': 'SERGIPE', '29': 'BAHIA', '31': 'MINAS GERAIS', '32': 'ESPIRITO SANTO', 
    '33': 'RIO DE JANEIRO', '35': 'SAO PAULO', '41': 'PARANA', '42': 'SANTA CATARINA', 
    '43': 'RIO GRANDE DO SUL', '50': 'MATO GROSSO DO SUL', '51': 'MATO GROSSO', 
    '52': 'GOIAS', '53': 'DISTRITO FEDERAL'
}

print("1. Lendo GeoJSON...")
gdf = gpd.read_file(INPUT_GEOJSON)

# ==========================================
# 2. Normalização e Nova Coluna: municipios_nome
# ==========================================
gdf['codigo_ibge'] = gdf['id'].astype(str).str.strip()
gdf['cod_estado'] = gdf['codigo_ibge'].str[:2]
gdf['estado_nome'] = gdf['cod_estado'].map(mapa_estados)

# Aplicando a padronização na nova coluna prioritária
gdf['municipios_nome'] = gdf['name'].apply(remover_acentos_e_sujeira)

# Chave de ligação reconstruída com o novo nome
gdf['chave_localidade'] = gdf['estado_nome'] + "_" + gdf['municipios_nome']

# ==========================================
# 3. Geometria (WKT e Centróide)
# ==========================================
if gdf.crs is None: gdf.set_crs(epsg=4326, inplace=True)
gdf_plano = gdf.to_crs(epsg=3857)
gdf['centroid'] = gdf_plano.geometry.centroid.to_crs(epsg=4326)
gdf['lat_centroide'] = gdf['centroid'].y.round(6)
gdf['lon_centroide'] = gdf['centroid'].x.round(6)
gdf['geometria_wkt'] = gdf.geometry.to_wkt()

# ==========================================
# 4. Exportação
# ==========================================
df_dim = pd.DataFrame(gdf.drop(columns=['geometry', 'centroid', 'cod_estado']))
colunas_finais = [
    'codigo_ibge', 
    'chave_localidade', 
    'estado_nome', 
    'municipios_nome', # <-- Coluna Atualizada
    'lat_centroide', 
    'lon_centroide', 
    'geometria_wkt'
]
df_dim = df_dim[colunas_finais].dropna(subset=['codigo_ibge'])
df_dim.to_csv(OUTPUT_CSV, index=False, encoding='utf-8', lineterminator='\n')

print(f"✅ Dimensão prioritária salva: {OUTPUT_CSV}")