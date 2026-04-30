# src/streaming/produtor_focos.py

import json
import time
import pandas as pd
from kafka import KafkaProducer
import sys
import os


# Adiciona o diretório RAIZ do projeto ao path do sistema
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.spatial.motor_espacial import MotorEspacial
from src.streaming.processador import ProcessadorFocos

# ==========================================
# Configurações do Ambiente de Streaming
# ==========================================
# Porta exposta do Docker Compose (Onde o Kafka ouve requisições externas)
KAFKA_BROKER = 'localhost:29092'
TOPICO_KAFKA = 'streaming_focos_incendio'
CAMINHO_BRUTO = os.path.join('data', 'raw', 'focos_incendio_brutos.csv')

def iniciar_produtor():
    """Inicializa as instâncias e gerencia o loop principal de ingestão."""
    print("🔌 Conectando ao cluster Kafka...")
    try:
        # Configura o Kafka para converter automaticamente os dicionários Python em formato JSON (UTF-8)
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Erro ao conectar no Kafka: {e}")
        sys.exit(1)

    # Inicia a arquitetura em memória (Demora alguns segundos para subir as árvores espaciais)
    motor = MotorEspacial()
    processador = ProcessadorFocos(motor)

    print("🚀 Iniciando envio para o tópico Kafka...")
    
    try:
        # Simula a chegada contínua de dados de uma API lendo o CSV linha a linha
        df_bruto = pd.read_csv(CAMINHO_BRUTO)
        
        for _, linha in df_bruto.iterrows():
            
            # Delega a responsabilidade de limpeza e matemática para a classe Processador
            payload_final = processador.transformar(linha.to_dict())
            
            # Despacha o pacote finalizado para o barramento do Kafka
            producer.send(TOPICO_KAFKA, value=payload_final)
            
            # Feedback visual no terminal (opcional)
            print(f"🔥 Enviado: {payload_final['chave_localidade']} | Risco Via: {payload_final['distancia_rodovia_km']}km")
            
            # Simula a latência de streaming para não flodar o terminal de uma vez (remover em produção)
            time.sleep(0.1) 
            
    except FileNotFoundError:
        print(f"❌ Arquivo {CAMINHO_BRUTO} não encontrado. Verifique o caminho.")

    # Garante que as últimas mensagens presas no buffer da memória sejam enviadas antes do script morrer
    producer.flush()
    print("✅ Fluxo de streaming encerrado com sucesso.")

if __name__ == "__main__":
    iniciar_produtor()