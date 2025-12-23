import json
import time
from kafka import KafkaProducer

# Importa nossas ferramentas de instrumentação
from instrumentation import monitor_performance, MetricsTracker

# --- CONFIGURAÇÕES ---
TOPIC_DATA = 'clima-sudeste-raw'
KAFKA_BOOTSTRAP = 'localhost:9092'

CITIES = [
    {"name": "Sao Paulo", "lat": "-23.5505", "lon": "-46.6333"},
    {"name": "Itajuba",   "lat": "-22.4256", "lon": "-45.4582"},
    {"name": "Rio de Janeiro", "lat": "-22.9068", "lon": "-43.1729"},
    {"name": "Belo Horizonte", "lat": "-19.9167", "lon": "-43.9345"}
]

# Inicializa o Producer de DADOS (O de métricas é automático no instrumentation.py)
data_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

import requests

# --- APLICAÇÃO DAS ANNOTATIONS ---

@monitor_performance("nasa_api_request")
def get_nasa_data(city):
    """
    Agora esta função envia métricas automaticamente!
    Não precisa mais de 'start_time' ou 'send_metric' aqui dentro.
    """
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    str_start = start_date.strftime('%Y%m%d')
    str_end = end_date.strftime('%Y%m%d')

    url = "https://power.larc.nasa.gov/api/temporal/hourly/point"
    params = {
        'parameters': 'T2M,RH2M,ALLSKY_SFC_SW_DWN,WS10M', 
        'community': 'RE',
        'longitude': city['lon'],
        'latitude': city['lat'],
        'start': str_start,
        'end': str_end,
        'format': 'JSON'
    }

    # O decorator vai cronometrar esta chamada
    response = requests.get(url, params=params, timeout=15)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API Error: {response.status_code}")

@monitor_performance("kafka_ingest")
def send_batch_to_kafka(city_name, records):
    """Função auxiliar isolada para podermos medir o tempo de envio do Kafka"""
    for record in records:
        data_producer.send(TOPIC_DATA, value=record)
    data_producer.flush() # Força envio para contar o tempo real de rede

def main():
    print("🚀 Iniciando Ingestão com Decorators...")
    
    # Garante que o tracker de métricas está ativo
    tracker = MetricsTracker()

    try:
        for city in CITIES:
            try:
                # 1. Chama a função decorada (gera métrica de API)
                data = get_nasa_data(city)

                if data:
                    # Processamento (Rápido, não precisa medir)
                    params = data['properties']['parameter']
                    temps = params.get('T2M', {})
                    humidities = params.get('RH2M', {})
                    solars = params.get('ALLSKY_SFC_SW_DWN', {})
                    winds = params.get('WS10M', {})
                    
                    records_buffer = []
                    for ts in temps:
                        if temps[ts] == -999: continue
                        record = {
                            "timestamp": ts,
                            "city": city['name'],
                            "location": {"lat": float(city['lat']), "lon": float(city['lon'])},
                            "temperature_c": temps.get(ts),
                            "humidity_percent": humidities.get(ts),
                            "solar_irradiance": solars.get(ts, 0),
                            "wind_speed_ms": winds.get(ts, 0)
                        }
                        records_buffer.append(record)

                    # 2. Chama a função decorada (gera métrica de Kafka)
                    send_batch_to_kafka({"name": city['name']}, records_buffer)
                    
                    print(f"✅ {city['name']}: Lote processado.")
                    time.sleep(1)

            except Exception as e:
                print(f"❌ Falha no ciclo de {city['name']}: {e}")

    finally:
        data_producer.close()
        tracker.close()
        print("🏁 Finalizado.")

if __name__ == "__main__":
    main()