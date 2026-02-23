import pandas as pd
import json
import time
import os
import math
from kafka import KafkaProducer

# --- CONFIGURAÇÕES ---
ARQUIVO_CSV_GRANDE = "bdqueimadas_2025-01-01_2025-12-10.csv"
TOPICO_KAFKA = "inpe-focos-calor"
BOOTSTRAP_SERVER = 'localhost:9092'
TAMANHO_DO_LOTE = 10000

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

# --- FUNÇÕES DE LIMPEZA (DATA CLEANING) ---
def safe_float(valor, default=0.0):
    """Converte para float aceitando vírgula, nulos e strings vazias."""
    try:
        if pd.isna(valor) or valor == '':
            return default
        # Troca vírgula por ponto (padrão PT-BR para US) e converte
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return default

def safe_int(valor, default=-1):
    """Converte para int lidando com '10.0', nulos e textos."""
    try:
        if pd.isna(valor) or valor == '':
            return default
        # Converte para float primeiro para aceitar "5.0" e depois para int
        return int(float(str(valor).replace(',', '.')))
    except (ValueError, TypeError):
        return default

def safe_str(valor, default="Desconhecido"):
    """Garante que seja string e remove espaços extras."""
    try:
        if pd.isna(valor) or str(valor).strip() == '':
            return default
        return str(valor).strip()
    except:
        return default

def processar_lote(df_chunk, lote_numero):
    """
    Transforma as colunas do INPE no formato JSON do Druid com tratamento intensivo.
    """
    registros_enviados = 0
    
    # Itera sobre as linhas do lote
    for index, row in df_chunk.iterrows():
        try:
            # 1. Tratamento CRÍTICO de Geo (Se falhar lat/lon, pula a linha)
            lat = safe_float(row.get('Latitude'), default=None)
            lon = safe_float(row.get('Longitude'), default=None)

            if lat is None or lon is None or lat == 0.0 or lon == 0.0:
                continue # Pula registro sem localização válida

            # 2. Tratamento de Data
            raw_date = str(row.get('DataHora', ''))
            # Ajuste básico de formatação
            data_iso = raw_date.replace('/', '-').strip()
            if ' ' in data_iso:
                data_iso = data_iso.replace(' ', 'T')
            
            # Validação simples: se a data for muito curta (ex: "nan"), usa timestamp atual ou pula
            if len(data_iso) < 10:
                # Opcional: Usar data atual ou pular. Aqui vamos pular para não sujar o histórico.
                continue 

            # 3. Montagem do Payload com Funções Seguras
            payload = {
                "timestamp": data_iso,
                
                # Tratamento de Strings (Dimensões)
                "satelite": safe_str(row.get('Satelite'), "Satélite N/A"),
                "pais": safe_str(row.get('Pais'), "Brasil"),
                "estado": safe_str(row.get('Estado'), "N/A"),
                "municipio": safe_str(row.get('Municipio'), "N/A"),
                "bioma": safe_str(row.get('Bioma'), "Não Identificado"),
                
                # Tratamento Numérico (Métricas)
                "dias_sem_chuva": safe_int(row.get('DiaSemChuva'), -1),
                "precipitacao": safe_float(row.get('Precipitacao'), 0.0),
                "risco_fogo": safe_float(row.get('RiscoFogo'), 0.0),
                "frp": safe_float(row.get('FRP'), 0.0),
                
                # Objeto Geoespacial
                "location": {
                    "lat": lat,
                    "lon": lon
                }
            }

            # 4. Envio
            if producer:
                producer.send(TOPICO_KAFKA, value=payload)
            
            # Printa só o primeiro do lote pra gente conferir
            if registros_enviados == 0:
                print(f"📤 [Lote {lote_numero}] Ex: {payload['municipio']} | FRP: {payload['frp']} | Risco: {payload['risco_fogo']} | Chuva: {payload['dias_sem_chuva']}")

            registros_enviados += 1
            
        except Exception as e:
            # Captura erro em uma linha específica sem parar o script inteiro
            print(f"⚠️ Erro ao processar linha {index} no lote {lote_numero}: {e}")
            continue

    if producer:
        producer.flush()
    
    return registros_enviados

def main():
    print(f"🚀 Iniciando Ingestão Blindada de Queimadas")
    print(f"📂 Arquivo: {ARQUIVO_CSV_GRANDE}")

    if not os.path.exists(ARQUIVO_CSV_GRANDE):
        print(f"❌ Arquivo não encontrado. Verifique o nome.")
        return

    inicio = time.time()
    total = 0
    lote = 1

    # Leitura em Streaming com engine 'python' é mais lenta mas mais tolerante a erros de separador
    # Mas 'c' (padrão) é melhor para performance. Vamos manter padrão mas tratando encoding.
    try:
        with pd.read_csv(ARQUIVO_CSV_GRANDE, chunksize=TAMANHO_DO_LOTE, encoding='latin1', sep=',', on_bad_lines='skip') as reader:
            for df_chunk in reader:
                qtd = processar_lote(df_chunk, lote)
                total += qtd
                print(f"✅ Lote {lote} processado ({qtd} registros válidos). Total: {total}")
                lote += 1
    except Exception as e:
        print(f"❌ Erro fatal na leitura do CSV: {e}")
        print("Dica: Verifique se o encoding é 'utf-8' ou 'latin1' e se o separador é ',' ou ';'")

    tempo_final = time.time() - inicio
    print("-" * 50)
    print(f"🏁 Processamento Concluído!")
    print(f"🔥 Total de focos ingeridos: {total}")
    print(f"⏱️ Tempo total: {tempo_final:.2f} segundos")

if __name__ == "__main__":
    main()