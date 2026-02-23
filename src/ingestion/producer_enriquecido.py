import json
import time
import glob
import pandas as pd
from kafka import KafkaProducer
from motor_espacial import MotorEspacial

# =====================================================================
# CONFIGURAÇÕES DO BARRAMENTO (MESSAGE BROKER)
# =====================================================================
KAFKA_BROKER = 'localhost:9092'
TOPICO = 'queimadas_enriquecidas' # Tópico de destino para os payloads com inteligência espacial

def criar_produtor():
    """
    Inicializa o cliente Kafka Producer.
    Garante a serialização UTF-8 das mensagens, convertendo os dicionários Python 
    em payloads JSON estritos para consumo downstream pelo Apache Druid.
    """
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

def iniciar_streaming():
    """
    Orquestrador Principal do Pipeline de Streaming (DataOps).
    
    Responsabilidades:
    1. Instanciar o motor matemático espacial em memória.
    2. Estabelecer canal de comunicação contínuo com o broker Kafka.
    3. Higienizar e carregar os dados brutos de satélite (ETL leve).
    4. Aplicar processamento linha-a-linha simulando telemetria em tempo real.
    """
    print("="*60)
    print(" 🔥 INICIANDO PIPELINE DATAOPS DE INGESTÃO (STREAMING) 🔥")
    print("="*60)
    
    motor = MotorEspacial()
    
    print(f"\n📡 Estabelecendo handshake com Kafka Broker ({KAFKA_BROKER})...")
    try:
        producer = criar_produtor()
        print("✅ Link Kafka estabelecido!")
    except Exception as e:
        print(f"❌ Falha de rede. Verifique se o container Kafka está ativo. Erro: {e}")
        return

    # Busca dinâmica pelo ficheiro CSV de queimadas (padrão INPE)
    arquivos_csv = glob.glob("data/raw/bdqueimadas*.csv")
    if not arquivos_csv:
        print("❌ Ficheiro bruto de telemetria não localizado no repositório local.")
        return
        
    caminho_csv = arquivos_csv[0]
    print(f"📁 Efectuando parsing do ficheiro de satélite: {caminho_csv}")
    
    # --- MÓDULO DE HIGIENIZAÇÃO (DATA CLEANSING) ---
    # O motor 'python' permite inferência heurística do delimitador (vírgula vs. ponto-e-vírgula)
    df_focos = pd.read_csv(caminho_csv, sep=None, engine='python')
    
    # Padronização do esquema (Schema Normalization): força minúsculas e remove espaços residuais
    df_focos.columns = df_focos.columns.str.strip().str.lower()
    
    # Tratamento de variabilidade de nomenclatura de coordenadas da API do INPE
    if 'latitude' in df_focos.columns:
        df_focos.rename(columns={'latitude': 'lat', 'longitude': 'lon'}, inplace=True)
        
    if 'lat' not in df_focos.columns or 'lon' not in df_focos.columns:
        print(f"❌ ERRO DE SCHEMA: Coordenadas ausentes. Colunas encontradas: {list(df_focos.columns)}")
        return
        
    total_focos = len(df_focos)
    print(f"🛰️ Lote de telemetria inicializado: {total_focos} eventos agendados.")
    print("-" * 60)
    
    enviados = 0
    # --- LOOP DE STREAMING ---
    for index, row in df_focos.iterrows():
        try:
            # 1. Extracção de métricas base
            lat = float(row['lat'])
            lon = float(row['lon'])
            frp = float(row['frp']) if 'frp' in row else 0.0 # Fire Radiative Power
            data_hora = row['data_hora'] if 'data_hora' in row else "2025-01-01 00:00:00"
            
            # 2. Invocação do Motor Analítico (Enriquecimento In-Flight)
            payload_enriquecido = motor.enriquecer(lat, lon)
            
            # 3. Consolidação do Payload Final
            payload_enriquecido['data_hora'] = data_hora
            payload_enriquecido['frp'] = frp
            payload_enriquecido['satelite'] = row.get('satelite', 'Desconhecido')
            
            # 4. Publicação no Barramento (Publish)
            producer.send(TOPICO, value=payload_enriquecido)
            enviados += 1
            
            # 5. Monitorização Visual via Terminal
            if payload_enriquecido['risco_eletrico'] in ['ALTO', 'MÉDIO']:
                print(f"🚨 [ALERTA {payload_enriquecido['risco_eletrico']}] Município: {payload_enriquecido['municipio']} | " 
                      f"Ameaça a {payload_enriquecido['dist_linha_km']}km da {payload_enriquecido['linha_afetada']}")
            elif enviados % 100 == 0:
                print(f"⏳ Telemetria: {enviados}/{total_focos} eventos processados e ingeridos...")
                
            # Rate limiting artificial para simular fluxo orgânico de eventos (evita estrangulamento de IO)
            time.sleep(0.05)
            
        except Exception as e:
            # Tolerância a falhas: Registos corrompidos são reportados, mas não derrubam o pipeline
            print(f"⚠️ Anomalia no registo de índice {index}: {e}")
            continue

    # Confirma o esvaziamento do buffer de rede do produtor
    producer.flush()
    print("-" * 60)
    print(f"✅ Fim da transmissão. {enviados} pacotes enriquecidos entregues ao broker Kafka.")

if __name__ == "__main__":
    iniciar_streaming()