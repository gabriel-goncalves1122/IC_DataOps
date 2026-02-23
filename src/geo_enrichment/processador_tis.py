import pandas as pd
import geopandas as gpd
from shapely import wkt
import os

def processar_terras_indigenas():
    caminho_csv = "data/raw/tis_poligonais.csv"
    caminho_saida = "data/geo/terras_indigenas.geojson"
    
    if not os.path.exists(caminho_csv):
        print(f"❌ Arquivo não encontrado em {caminho_csv}. Verifique a pasta.")
        return

    print("⏳ Carregando dados brutos das Terras Indígenas (FUNAI/IBGE)...")
    df = pd.read_csv(caminho_csv)
    
    print("📐 Convertendo coordenadas textuais (WKT) para polígonos matemáticos...")
    # O Shapely transforma a string de texto num polígono calculável
    df['geometry'] = df['the_geom'].apply(wkt.loads)
    
    # O arquivo original do governo brasileiro usa EPSG:4674 (SIRGAS 2000)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4674")
    
    print("🌍 Padronizando projeção para WGS84 (Global - GPS)...")
    gdf = gdf.to_crs(epsg=4326) # Agora fala a mesma língua do INPE e da ANEEL
    
    # Filtramos apenas as colunas úteis para não pesar a nossa base (o GeoJSON ficará mais leve)
    colunas_uteis = ['terrai_nome', 'etnia_nome', 'uf_sigla', 'fase_ti', 'superficie_perimetro_ha', 'geometry']
    gdf_limpo = gdf[colunas_uteis]
    
    print("💾 Salvando na camada espacial...")
    gdf_limpo.to_file(caminho_saida, driver="GeoJSON")
    
    print(f"✅ Sucesso absoluto! {len(gdf_limpo)} Terras Indígenas convertidas e prontas para análise.")

if __name__ == "__main__":
    processar_terras_indigenas()