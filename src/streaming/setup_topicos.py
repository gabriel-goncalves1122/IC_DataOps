from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configurações
KAFKA_BROKER = 'localhost:9092'
NOME_DO_TOPICO = 'streaming_focos_incendio'

def configurar_kafka():
    print(f"🔌 Conectando ao Kafka Admin em {KAFKA_BROKER}...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='setup_script'
        )
        
        # Define as regras do tópico (1 partição, fator de replicação 1 para ambiente local)
        novo_topico = NewTopic(
            name=NOME_DO_TOPICO, 
            num_partitions=1, 
            replication_factor=1
        )
        
        print(f"🛠️ Tentando criar o tópico '{NOME_DO_TOPICO}'...")
        admin_client.create_topics(new_topics=[novo_topico], validate_only=False)
        print("✅ SUCESSO: Tópico criado e pronto para receber dados!")
        
    except TopicAlreadyExistsError:
        print(f"⚠️ AVISO: O tópico '{NOME_DO_TOPICO}' já existe no cluster.")
    except Exception as e:
        print(f"❌ ERRO CRÍTICO: Não foi possível configurar o Kafka. Detalhes: {e}")
        print("Certifique-se de que o container do Docker está rodando e a porta 9092 está exposta.")
    finally:
        if 'admin_client' in locals():
            admin_client.close()

if __name__ == '__main__':
    configurar_kafka()