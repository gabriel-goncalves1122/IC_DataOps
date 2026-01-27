import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

# --- CONFIGURAÇÕES ---
ARQUIVO_CSV_GRANDE = "bdqueimadas_2025-01-01_2025-12-10.csv"  # <-- Nome do seu arquivo
TOPICO_KAFKA = "inpe-focos-calor"
BOOTSTRAP_SERVER = 'localhost:9092'
TAMANHO_DO_LOTE = 10000  # Processa de 10 em 10 mil linhas

# Configura o Producer Real
try:
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("✅ Conectado ao Kafka com sucesso!")
except Exception as e:
    print(f"⚠️ Kafka não encontrado. Rodando em modo simulação (apenas print).")
    producer = None

def processar_lote(df_chunk, lote_numero):
    """
    Transforma as colunas do INPE no formato JSON do Druid
    """
    # Filtra apenas o que tem Latitude/Longitude válidos (Segurança)
    df_chunk = df_chunk.dropna(subset=['Latitude', 'Longitude'])
    
    # Trata valores nulos numéricos com 0.0 ou -1
    df_chunk['RiscoFogo'] = df_chunk['RiscoFogo'].fillna(0.0)
    df_chunk['FRP'] = df_chunk['FRP'].fillna(0.0)
    df_chunk['Precipitacao'] = df_chunk['Precipitacao'].fillna(0.0)

    registros_enviados = 0
    
    # Itera sobre as linhas do lote
    for index, row in df_chunk.iterrows():
        
        # 1. Tratamento de Data (De '2025/01/01 15:00:00' para '2025-01-01T15:00:00')
        # O Druid prefere ISO-8601 com traços e T
        data_iso = str(row['DataHora']).replace('/', '-')
        if ' ' in data_iso:
            data_iso = data_iso.replace(' ', 'T')

        # 2. Montagem do Payload (Mapeamento Coluna -> JSON)
        payload = {
            "timestamp": data_iso,
            "satelite": str(row['Satelite']),
            "pais": str(row['Pais']),
            "estado": str(row['Estado']),
            "municipio": str(row['Municipio']),
            "bioma": str(row['Bioma']),
            
            # Métricas Numéricas
            "dias_sem_chuva": int(row['DiaSemChuva']),
            "precipitacao": float(row['Precipitacao']),
            "risco_fogo": float(row['RiscoFogo']),
            "frp": float(row['FRP']), # Intensidade do fogo
            
            # Objeto Geoespacial para o Druid
            "location": {
                "lat": float(row['Latitude']),
                "lon": float(row['Longitude'])
            }
        }

        # 3. Envio
        if producer:
            producer.send(TOPICO_KAFKA, value=payload)
        
        # Printa só o primeiro do lote pra gente conferir
        if registros_enviados == 0:
            print(f"📤 [Lote {lote_numero}] Exemplo enviado: {payload['municipio']} | FRP: {payload['frp']} | Risco: {payload['risco_fogo']}")

        registros_enviados += 1

    if producer:
        producer.flush() # Garante que o Kafka recebeu tudo antes de seguir
    
    return registros_enviados

def main():
    print(f"🚀 Iniciando Ingestão de Queimadas (INPE)")
    print(f"📂 Arquivo: {ARQUIVO_CSV_GRANDE}")

    if not os.path.exists(ARQUIVO_CSV_GRANDE):
        print(f"❌ Arquivo não encontrado. Verifique o nome.")
        return

    inicio = time.time()
    total = 0
    lote = 1

    # Leitura em Streaming (Chunks)
    with pd.read_csv(ARQUIVO_CSV_GRANDE, chunksize=TAMANHO_DO_LOTE, encoding='latin1', sep=',') as reader:
        for df_chunk in reader:
            qtd = processar_lote(df_chunk, lote)
            total += qtd
            print(f"✅ Lote {lote} processado ({qtd} registros). Total: {total}")
            lote += 1
            # time.sleep(0.1) # Descomente se quiser ver rodando devagar

    tempo_final = time.time() - inicio
    print("-" * 50)
    print(f"🏁 Processamento Concluído!")
    print(f"🔥 Total de focos ingeridos: {total}")
    print(f"⏱️ Tempo total: {tempo_final:.2f} segundos")

if __name__ == "__main__":
    main()