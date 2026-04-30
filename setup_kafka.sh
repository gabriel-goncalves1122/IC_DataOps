#!/bin/bash

# ==========================================
# Variáveis de Configuração
# ==========================================
CONTAINER_KAFKA="kafka"
TOPICO="streaming_focos_incendio"
BROKER="localhost:9092"

echo "=================================================="
echo "🛠️  INICIANDO SETUP DO KAFKA VIA DOCKER"
echo "=================================================="

echo "1. Verificando se o container '$CONTAINER_KAFKA' está rodando..."
if [ "$(docker ps -q -f name=^${CONTAINER_KAFKA}$)" ]; then
    echo "✅ Container '$CONTAINER_KAFKA' encontrado e operando."
else
    echo "❌ ERRO: Container '$CONTAINER_KAFKA' não está rodando. Inicie o Docker Compose primeiro."
    exit 1
fi

echo ""
echo "2. Criando o tópico '$TOPICO' (se não existir)..."
docker exec $CONTAINER_KAFKA kafka-topics \
    --create \
    --if-not-exists \
    --topic $TOPICO \
    --bootstrap-server $BROKER \
    --partitions 1 \
    --replication-factor 1

echo ""
echo "3. Listando tópicos disponíveis no cluster:"
docker exec $CONTAINER_KAFKA kafka-topics --list --bootstrap-server $BROKER

echo ""
echo "=================================================="
echo "✅ Setup concluído com sucesso!"
echo "=================================================="
echo "👉 PRÓXIMOS PASSOS:"
echo "1. Para VER os dados chegando, abra um novo terminal e rode o consumidor:"
echo "   docker exec -it $CONTAINER_KAFKA kafka-console-consumer --topic $TOPICO --bootstrap-server $BROKER"
echo ""
echo "2. Para ENVIAR os dados, rode o seu script Python (lembre-se de usar a porta 29092 no arquivo Python):"
echo "   python src/streaming/produtor_focos.py"
echo "=================================================="