import json
import time
import requests
import random
from kafka import KafkaProducer
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES ---
KAFKA_TOPIC = 'clima-sudeste-raw'  
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Lista de Cidades para Monitorar
CITIES = [
    {"name": "Sao Paulo", "lat": "-23.5505", "lon": "-46.6333"},
    {"name": "Itajuba",   "lat": "-22.4256", "lon": "-45.4582"}, # UNIFEI
    {"name": "Rio de Janeiro", "lat": "-22.9068", "lon": "-43.1729"},
    {"name": "Belo Horizonte", "lat": "-19.9167", "lon": "-43.9345"}
]

def get_nasa_data(city, days_back=7):
    """Busca dados da NASA para uma cidade específica"""
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    str_start = start_date.strftime('%Y%m%d')
    str_end = end_date.strftime('%Y%m%d')

    print(f"📡 Buscando dados para {city['name']}...")

    # Parameters: 
    # T2M (Temperatura), RH2M (Umidade), ALLSKY_SFC_SW_DWN (Irradiação Solar)
    # WS10M (Velocidade do Vento a 10m)
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

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Erro API ({city['name']}): {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")
        return None

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("🚀 Iniciando Ingestão Multi-Cidades...")

    for city in CITIES:
        data = get_nasa_data(city)

        if data:
            try:
                params = data['properties']['parameter']
                temps = params.get('T2M', {})
                humidities = params.get('RH2M', {})
                solars = params.get('ALLSKY_SFC_SW_DWN', {}) # Novo campo
                winds = params.get('WS10M', {})              # Novo campo
                
                count = 0
                for timestamp_key in temps:
                    # Validação básica de dados (-999 é erro/nulo na NASA)
                    if temps[timestamp_key] == -999:
                        continue

                    payload = {
                        "timestamp": timestamp_key,
                        "city": city['name'],
                        "location": {"lat": float(city['lat']), "lon": float(city['lon'])},
                        "temperature_c": temps.get(timestamp_key),
                        "humidity_percent": humidities.get(timestamp_key),
                        "solar_irradiance": solars.get(timestamp_key, 0),
                        "wind_speed_ms": winds.get(timestamp_key, 0),
                        "sensor_id": f"nasa-v2-{city['name'][:3].lower()}" # Simulando ID de sensor
                    }

                    producer.send(KAFKA_TOPIC, value=payload)
                    count += 1
                
                print(f"✅ {city['name']}: {count} registros enviados.")
                
                # Pausa para não bater no Rate Limit da API da NASA (importante!)
                time.sleep(2) 

            except KeyError as e:
                print(f"❌ Erro de estrutura JSON para {city['name']}: {e}")

    producer.flush()
    producer.close()
    print("🏁 Ciclo finalizado com sucesso.")

if __name__ == "__main__":
    main()