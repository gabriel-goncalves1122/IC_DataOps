import json
import time
import glob
import os
import pandas as pd
from kafka import KafkaProducer
from motor_espacial import MotorEspacial

# =====================================================================
# CONFIGURAÇÕES DO BARRAMENTO E CONTROLE DE ESTADO
# =====================================================================
KAFKA_BROKER = 'localhost:29092'
TOPICO = 'focos_calor' 
CHECKPOINT_FILE = 'data/raw/checkpoint_ingestao.txt'

def criar_produtor():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
        linger_ms=50,             
        batch_size=65536,         
        compression_type='gzip',  
        acks=1                    
    )

def ler_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return -1
    return -1

def salvar_checkpoint(index):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(index))

def iniciar_streaming():
    print("="*60)
    print(" 🔥 PIPELINE DATAOPS: INGESTÃO MASSIVA (MODO TURBO) 🔥")
    print("="*60)
    print("💡 DICA: Pressione 'Ctrl + C' a qualquer momento para pausar de forma segura.")
    
    motor = MotorEspacial()
    
    print(f"\n📡 Conectando ao Kafka Broker ({KAFKA_BROKER})...")
    try:
        producer = criar_produtor()
    except Exception as e:
        print(f"❌ Falha de rede. Kafka ativo? Erro: {e}")
        return

    arquivos_csv = glob.glob("data/raw/bdqueimadas*.csv")
    if not arquivos_csv:
        print("❌ Ficheiro bruto não localizado em data/raw/")
        return
        
    caminho_csv = arquivos_csv[0]
    print(f"📁 Lendo arquivo: {caminho_csv}")
    
    df_focos = pd.read_csv(caminho_csv, sep=None, engine='python')
    df_focos.columns = df_focos.columns.str.strip().str.lower()
    df_focos = df_focos.fillna('Desconhecido') 
    
    if 'latitude' in df_focos.columns:
        df_focos.rename(columns={'latitude': 'lat', 'longitude': 'lon'}, inplace=True)
        
    total_focos = len(df_focos)

    # --- IDENTIFICAÇÃO DINÂMICA DA COLUNA DE TEMPO ---
    coluna_tempo_alvo = None
    possiveis_nomes = ['datahora', 'data_hora', 'timestamp', 'data']
    for nome in possiveis_nomes:
        if nome in df_focos.columns:
            coluna_tempo_alvo = nome
            print(f"✅ Coluna de tempo identificada automaticamente: '{coluna_tempo_alvo}'")
            break
            
    if not coluna_tempo_alvo:
        print("⚠️ AVISO: Nenhuma coluna de data padrão encontrada. O timestamp de fallback será usado e causará anomalias no Druid.")

    # --- SISTEMA DE RETOMADA (RESUME) ---
    ultimo_index_processado = ler_checkpoint()
    
    if ultimo_index_processado >= 0:
        print(f"🔄 Retomando processo a partir da linha {ultimo_index_processado + 1}...")
        df_focos = df_focos.iloc[ultimo_index_processado + 1:]
    else:
        print(f"🚀 Iniciando processamento do zero ({total_focos} eventos).")

    print("-" * 60)
    
    enviados = 0
    linha_atual = ultimo_index_processado
    tempo_inicio_lote = time.time()
    
    try:
        for index, row in df_focos.iterrows():
            linha_atual = index
            
            try:
                lat = float(row['lat'])
                lon = float(row['lon'])
                
                frp_raw = row.get('frp', 0.0)
                frp = float(frp_raw) if frp_raw != 'Desconhecido' else 0.0
                
                # =======================================================
                # TRATAMENTO DE DATAOPS: NORMALIZAÇÃO PARA ISO 8601
                # =======================================================
                data_hora_raw = row[coluna_tempo_alvo] if coluna_tempo_alvo else "2025-01-01 00:00:00"
                data_hora_str = str(data_hora_raw) if data_hora_raw != 'Desconhecido' else "2025-01-01 00:00:00"
                
                try:
                    # O Pandas converte qualquer formato (com barras, espaços, etc) para Datetime
                    # O isoformat() devolve exatamente o padrão "YYYY-MM-DDTHH:MM:SS"
                    data_hora = pd.to_datetime(data_hora_str).isoformat() + "Z"
                except Exception:
                    # Se a data do CSV estiver completamente corrompida, usamos o fallback seguro
                    data_hora = "2025-01-01T00:00:00Z"
                # =======================================================
                
                payload_enriquecido = motor.enriquecer(lat, lon)
        
                payload_enriquecido['data_hora'] = data_hora
                payload_enriquecido['frp'] = frp
                payload_enriquecido['satelite'] = row.get('satelite', 'Desconhecido')
                payload_enriquecido['estado'] = row.get('estado', 'Desconhecido') 
                payload_enriquecido['bioma'] = row.get('bioma', 'Desconhecido')
                
                producer.send(TOPICO, value=payload_enriquecido)
                enviados += 1
                    
                if enviados % 5000 == 0:
                    salvar_checkpoint(linha_atual)
                    decorrido = time.time() - tempo_inicio_lote
                    taxa = 5000 / decorrido
                    print(f"⚡ Progresso: {linha_atual + 1}/{total_focos} | Velocidade: {taxa:.0f} msgs/seg")
                    tempo_inicio_lote = time.time()

            except Exception as e:
                print(f"⚠️ Anomalia no registro {index}: {e}")
                continue

        print("⏳ Esvaziando buffer final do Kafka...")
        producer.flush()
        print("-" * 60)
        print(f"✅ Ingestão finalizada com sucesso! Todos os {total_focos} focos foram enviados.")
        
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)

    except KeyboardInterrupt:
        print("\n\n🛑 SINAL DE INTERRUPÇÃO RECEBIDO (Ctrl+C).")
        print(f"💾 Salvando o progresso na linha {linha_atual} antes de desligar...")
        salvar_checkpoint(linha_atual)
        producer.flush()
        print("😴 Desligamento seguro concluído. Você pode retomar a qualquer momento.")
        return

if __name__ == "__main__":
    iniciar_streaming()