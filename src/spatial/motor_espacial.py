# src/spatial/motor_espacial.py

import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
from src.config import settings

class MotorEspacial:
    """
    Motor analítico em memória responsável por enriquecer dados de GPS (Lat/Lon) brutos
    com contexto territorial (intersecções) e de infraestrutura (distância mínima).
    """
    def __init__(self):
        print("🌍 Inicializando Motor Espacial Avançado (Leitura de Dimensões WKT)...")
        
        # 1. Malhas de Intersecção (Polígonos)
        # Mantidas em EPSG:4326 (Padrão GPS de graus decimais) pois só queremos saber se "está dentro ou fora".
        self.gdf_funai = self._carregar_dimensao(settings.DIM_FUNAI, "FUNAI")
        self.gdf_municipios = self._carregar_dimensao(settings.DIM_MUNICIPIOS_COORD, "Municípios Coordenadas")
        
        # 2. Malhas de Ameaça/Distância (Linhas e Pontos)
        # Projetadas para EPSG:5880 (SIRGAS 2000 / Sistema Métrico Brasileiro).
        # Necessário porque calcular distância em graus (EPSG:4326) gera distorções matemáticas.
        self.gdf_rodovias = self._carregar_dimensao(settings.DIM_RODOVIAS, "Rodovias", projetar_metrico=True)
        self.gdf_linhas = self._carregar_dimensao(settings.DIM_LINHAS_ONS, "Linhas ONS", projetar_metrico=True)
        self.gdf_subestacoes = self._carregar_dimensao(settings.DIM_SUBESTACOES, "Subestações ONS", projetar_metrico=True)
        self.gdf_sistemas = self._carregar_dimensao(settings.DIM_SISTEMAS_ISO, "Sistemas Isolados", projetar_metrico=True)
        self.gdf_hospitais = self._carregar_dimensao(settings.DIM_HOSPITAIS, "Hospitais CNES", projetar_metrico=True)

    def _carregar_dimensao(self, path, nome, projetar_metrico=False):
        """
        Lê o CSV da dimensão tratada e converte para GeoDataFrame, identificando
        automaticamente se a geometria está em texto (WKT) ou colunas Lat/Lon.
        """
        try:
            df = pd.read_csv(path)
            
            # Inteligência de parsing: verifica qual formato espacial a dimensão usa
            if 'geometria_wkt' in df.columns:
                df['geometry'] = df['geometria_wkt'].apply(lambda x: wkt.loads(str(x)) if pd.notna(x) else None)
            elif 'longitude' in df.columns and 'latitude' in df.columns:
                df['geometry'] = gpd.points_from_xy(df['longitude'], df['latitude'])
            else:
                print(f"⚠️ Nenhuma coluna espacial encontrada em {nome}.")
                return None
            
            # Remove lixo: garante que o motor não quebre tentando calcular distância de geometria nula
            df = df.dropna(subset=['geometry'])
            
            # Cria a malha espacial baseada no GPS global
            gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
            
            # Se for calcular raio/distância, converte de Graus para Metros
            if projetar_metrico:
                gdf = gdf.to_crs(epsg=5880)
            
            # CRÍTICO PARA PERFORMANCE: Cria o índice R-Tree. 
            # Sem isso, o cálculo de distância compararia 1 foco vs todas as rodovias do Brasil.
            # Com isso, ele compara apenas com o quadrante geográfico correto em microssegundos.
            gdf.sindex 
            
            print(f"✅ Dimensão '{nome}' indexada. Total: {len(gdf)} registros.")
            return gdf
        except Exception as e:
            print(f"⚠️ Erro ao carregar '{nome}': {e}")
            return None

    def _distancia_proxima(self, gdf_alvo, ponto_metrico, col_id):
        """Busca o elemento mais próximo na R-Tree e retorna o ID e a distância em KM."""
        if gdf_alvo is None or gdf_alvo.empty:
            return -1, -1.0
            
        # sindex.nearest retorna uma tupla. Pegamos o índice [1][0] que representa a linha mais próxima
        idx_nearest = gdf_alvo.sindex.nearest(ponto_metrico)[1][0]
        geom_alvo = gdf_alvo.iloc[idx_nearest].geometry
        
        # Pega a distância em metros e divide por 1000 para KM
        dist_km = ponto_metrico.distance(geom_alvo) / 1000.0
        
        return int(gdf_alvo.iloc[idx_nearest].get(col_id, -1)), round(dist_km, 2)

    def analisar_todas_ameacas(self, lat, lon):
        """
        Orquestra a análise completa de um foco de incêndio.
        Retorna um dicionário com todas as Chaves Estrangeiras (FKs) espaciais prontas para a Tabela de Fatos.
        """
        # Template de resposta segura caso a coordenada seja inválida
        res = {
            'codigo_ti': -1,
            'codigo_ibge_espacial': -1,
            'id_trecho_rodoviario': -1, 'distancia_rodovia_km': -1.0,
            'id_linha_transmissao': -1, 'distancia_linha_km': -1.0,
            'id_subestacao': -1, 'distancia_subestacao_km': -1.0,
            'id_sistema_isolado': -1, 'distancia_sistema_isolado_km': -1.0,
            'codigo_cnes_prox': -1, 'distancia_hospital_km': -1.0
        }

        if pd.isna(lat) or pd.isna(lon): return res

        # 1. Objeto geográfico bruto em graus
        ponto_gps = Point(float(lon), float(lat))

        # --- ANÁLISE DE INTERSECÇÃO (Dentro ou Fora?) ---
        if self.gdf_funai is not None:
            match_ti = self.gdf_funai.sindex.query(ponto_gps, predicate="intersects")
            if len(match_ti) > 0: res['codigo_ti'] = int(self.gdf_funai.iloc[match_ti[0]]['codigo_ti'])

        if self.gdf_municipios is not None:
            match_mun = self.gdf_municipios.sindex.query(ponto_gps, predicate="intersects")
            if len(match_mun) > 0: res['codigo_ibge_espacial'] = int(self.gdf_municipios.iloc[match_mun[0]]['codigo_ibge'])

        # --- ANÁLISE DE PROXIMIDADE (Buffer de Ameaça) ---
        # Converte o ponto UMA ÚNICA VEZ para o sistema métrico, economizando processamento da CPU
        ponto_metrico = gpd.GeoSeries([ponto_gps], crs="EPSG:4326").to_crs(epsg=5880).iloc[0]

        # Varre todas as camadas de infraestrutura buscando o vizinho mais próximo
        res['id_trecho_rodoviario'], res['distancia_rodovia_km'] = self._distancia_proxima(self.gdf_rodovias, ponto_metrico, 'id_trecho_rodoviario')
        res['id_linha_transmissao'], res['distancia_linha_km'] = self._distancia_proxima(self.gdf_linhas, ponto_metrico, 'id_linha_transmissao')
        res['id_subestacao'], res['distancia_subestacao_km'] = self._distancia_proxima(self.gdf_subestacoes, ponto_metrico, 'id_subestacao')
        res['id_sistema_isolado'], res['distancia_sistema_isolado_km'] = self._distancia_proxima(self.gdf_sistemas, ponto_metrico, 'id_sistema_isolado')
        res['codigo_cnes_prox'], res['distancia_hospital_km'] = self._distancia_proxima(self.gdf_hospitais, ponto_metrico, 'codigo_cnes')

        return res