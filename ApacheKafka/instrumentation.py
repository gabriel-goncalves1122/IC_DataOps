import time
import json
import functools
from kafka import KafkaProducer
from datetime import datetime

# Configurações Padrão
METRICS_TOPIC = 'system-metrics'
BOOTSTRAP_SERVER = 'localhost:9092'

class MetricsTracker:
    _instance = None

    def __new__(cls):
        """Padrão Singleton para garantir apenas uma conexão com o Kafka"""
        if cls._instance is None:
            cls._instance = super(MetricsTracker, cls).__new__(cls)
            cls._instance.producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        return cls._instance

    def send_metric(self, name, value, unit, tags=None):
        """Envia o payload para o Kafka"""
        try:
            payload = {
                "timestamp": datetime.now().isoformat(),
                "metric_name": name,
                "value": value,
                "unit": unit,
                "tags": tags or {},
                "service": "python-producer"
            }
            # Envia assincronamente (fire and forget) para não travar o app principal
            self.producer.send(METRICS_TOPIC, value=payload)
        except Exception as e:
            print(f"⚠️ Falha ao enviar métrica: {e}")

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()

# --- AQUI ESTÁ A "ANNOTATION" (DECORATOR) ---
def monitor_performance(metric_name):
    """
    Decorator para medir tempo de execução e taxa de sucesso/erro.
    Uso: @monitor_performance("nome_da_metrica")
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracker = MetricsTracker() # Pega a instância única
            start_time = time.time()
            status = "success"
            error_msg = None
            
            # Tenta extrair o nome da cidade dos argumentos (se existir)
            # Isso é específico para o seu caso onde passamos um dict 'city'
            tag_info = "unknown"
            if args and isinstance(args[0], dict) and 'name' in args[0]:
                tag_info = args[0]['name']
            
            try:
                # Executa a função original
                result = func(*args, **kwargs)
                return result
            
            except Exception as e:
                status = "error"
                error_msg = str(e)
                raise e # Relança o erro para não esconder falhas críticas
            
            finally:
                # Calcula duração e envia métrica SEMPRE (sucesso ou erro)
                duration_ms = (time.time() - start_time) * 1000
                
                tags = {
                    "status": status,
                    "target": tag_info
                }
                
                # 1. Métrica de Latência
                tracker.send_metric(f"{metric_name}_latency", duration_ms, "ms", tags)
                
                # 2. Métrica de Contagem (Throughput)
                tracker.send_metric(f"{metric_name}_count", 1, "count", tags)

        return wrapper
    return decorator