import os

# Caminhos do Sistema
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DIM_DIR = os.path.join(DATA_DIR, 'processed', 'dimensions')

# Focos Brutos
RAW_FOCOS = os.path.join(DATA_DIR, 'raw', 'focos_incendio_brutos.csv')

# Polígonos (Intersecção)
DIM_FUNAI = os.path.join(DIM_DIR, 'dim_terras_indigenas_funai.csv')
DIM_MUNICIPIOS_COORD = os.path.join(DIM_DIR, 'dim_municipios_coordenadas.csv')
# Se tiver geometria de quilombos separada:
# DIM_QUILOMBOS = os.path.join(DIM_DIR, 'dim_quilombos_geometria.csv') 

# Linhas e Pontos (Proximidade / Raio de Ameaça)
DIM_RODOVIAS = os.path.join(DIM_DIR, 'dim_rodovias_federais.csv')
DIM_LINHAS_ONS = os.path.join(DIM_DIR, 'dim_ons_linhas_transmissao.csv')
DIM_SUBESTACOES = os.path.join(DIM_DIR, 'dim_ons_subestacoes.csv')
DIM_SISTEMAS_ISO = os.path.join(DIM_DIR, 'dim_ons_sistemas_isolados.csv')
DIM_HOSPITAIS = os.path.join(DIM_DIR, 'dim_hospitais_cnes.csv')

# Configurações Kafka
KAFKA_BROKER = 'localhost:29092'
TOPICO_KAFKA = 'streaming_focos_incendio'