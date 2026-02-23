import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import warnings

# Suprime avisos não críticos de dependências (ex: PyArrow) para manter o terminal limpo no streaming
warnings.filterwarnings('ignore')

class MotorEspacial:
    """
    Motor de Processamento Espacial em Memória (In-Memory Spatial Engine).
    
    Esta classe atua como o 'cérebro' geográfico do pipeline. Ela carrega polígonos 
    e vetores complexos para a memória RAM (RAM-bound processing) apenas uma vez 
    durante a inicialização (cold start). 
    
    Utiliza indexação espacial (R-Tree) para realizar cálculos de intersecção 
    e proximidade em frações de milissegundo, evitando gargalos no banco de dados OLAP.
    """
    
    def __init__(self):
        print("⚙️ [Motor Espacial] Inicializando infraestrutura de geometria analítica...")
        
        try:
            # 1. Carregamento de Polígonos baseados em Graus (EPSG:4326 - WGS84)
            # Mantemos em graus pois a operação de intersecção (Point in Polygon) não exige conversão métrica.
            self.gdf_mun = gpd.read_file("data/geo/municipios_br.geojson")
            self.gdf_tis = gpd.read_file("data/geo/terras_indigenas.geojson")
            
            # 2. Carregamento da Infraestrutura Crítica
            gdf_linhas = gpd.read_file("data/geo/linhas_transmissao_ons.geojson")
            gdf_subs = gpd.read_file("data/geo/subestacoes_ons.geojson")
            
            print("⚙️ [Motor Espacial] Projetando malha elétrica para métrica plana (EPSG:5880)...")
            # Projeção Policônica do Brasil (EPSG:5880): Converte coordenadas geográficas (Lat/Lon) 
            # para um plano Cartesiano (Metros). Fundamental para o cálculo exato de distâncias.
            self.gdf_linhas_metric = gdf_linhas.to_crs(epsg=5880)
            self.gdf_subs_metric = gdf_subs.to_crs(epsg=5880)
            
            print("⚙️ [Motor Espacial] Construindo Árvores-R (Spatial Indexes) para O(log N)...")
            # A invocação de .sindex força a criação de um índice espacial na memória em C++ (via pyogrio/rtree).
            # Isso garante que a busca de vizinhos não faça um 'full table scan', garantindo baixa latência.
            self.gdf_mun.sindex
            self.gdf_tis.sindex
            self.gdf_linhas_metric.sindex
            self.gdf_subs_metric.sindex
            
            print("✅ [Motor Espacial] Motor aquecido e operacional.")
            
        except Exception as e:
            print(f"❌ [Motor Espacial] Falha crítica de alocação espacial. Erro: {e}")
            raise e

    def enriquecer(self, lat, lon):
        """
        Recebe coordenadas puras de um sensor/satélite e devolve um dicionário de contexto rico.
        
        Realiza duas operações topológicas centrais:
        1. sjoin (Spatial Join - Predicado 'intersects'): Identifica área de abrangência.
        2. sjoin_nearest (Nearest Neighbor): Calcula distância métrica da infraestrutura mais próxima.
        """
        # Constrói o ponto geométrico a partir das coordenadas ingeridas
        ponto = Point(lon, lat)
        gdf_foco = gpd.GeoDataFrame(geometry=[ponto], crs="EPSG:4326")
        
        # --- ETAPA 1: CONTEXTO SÓCIO-GEOGRÁFICO (Graus) ---
        join_mun = gpd.sjoin(gdf_foco, self.gdf_mun, how="left", predicate="intersects")
        municipio = join_mun['name'].iloc[0] if 'name' in join_mun.columns and not pd.isna(join_mun['name'].iloc[0]) else "Desconhecido"
        
        join_ti = gpd.sjoin(gdf_foco, self.gdf_tis, how="left", predicate="intersects")
        em_ti = not pd.isna(join_ti['terrai_nome'].iloc[0]) if 'terrai_nome' in join_ti.columns else False
        nome_ti = join_ti['terrai_nome'].iloc[0] if em_ti else "Nenhuma"
        
        # --- ETAPA 2: AVALIAÇÃO DE RISCO FÍSICO (Metros) ---
        # Projeção instantânea do foco isolado para o plano métrico
        gdf_foco_metric = gdf_foco.to_crs(epsg=5880)
        
        # Identifica a Linha de Transmissão mais próxima e a respectiva distância
        join_linha = gpd.sjoin_nearest(gdf_foco_metric, self.gdf_linhas_metric, how="left", distance_col="dist_metros")
        dist_linha_km = join_linha['dist_metros'].iloc[0] / 1000 if not join_linha.empty else 9999
        nome_linha = join_linha['Name'].iloc[0] if not join_linha.empty else "N/A"
        voltagem_linha = join_linha['Voltagem'].iloc[0] if 'Voltagem' in join_linha.columns else "N/A"
        
        # Identifica a Subestação mais próxima
        join_sub = gpd.sjoin_nearest(gdf_foco_metric, self.gdf_subs_metric, how="left", distance_col="dist_metros")
        dist_sub_km = join_sub['dist_metros'].iloc[0] / 1000 if not join_sub.empty else 9999
        nome_sub = join_sub['Name'].iloc[0] if not join_sub.empty else "N/A"
        
        # --- ETAPA 3: MOTOR DE REGRAS (Business Logic) ---
        # Classificação de severidade baseada na proximidade (<5km = ALTO; <15km = MÉDIO)
        risco = "ALTO" if (dist_linha_km <= 5.0 or dist_sub_km <= 5.0) else ("MÉDIO" if (dist_linha_km <= 15.0 or dist_sub_km <= 15.0) else "BAIXO")

        return {
            "lat": lat,
            "lon": lon,
            "municipio": municipio,
            "em_terra_indigena": bool(em_ti),
            "nome_ti": nome_ti,
            "dist_linha_km": round(dist_linha_km, 2),
            "linha_afetada": nome_linha,
            "voltagem_linha": voltagem_linha,
            "dist_subestacao_km": round(dist_sub_km, 2),
            "subestacao_afetada": nome_sub,
            "risco_eletrico": risco
        }