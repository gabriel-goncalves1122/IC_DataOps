import time
import csv
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

# === CONFIGURAÇÕES ===
TOPIC = 'meu_topico'
NUM_MSGS = 1_000_000
CSV_METRICS = "consumer_metrics.csv"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='grupo-teste',
    enable_auto_commit=True
)

print("Consumer iniciado! Aguardando mensagens...\n")

# === Métricas ===
start_time = time.time()
last_time = start_time
received_count = 0

batch_rates = []
batch_elapsed = []

# === Loop principal ===
for message in consumer:
    received_count += 1

    # métricas por batch de 1000
    if received_count % 1000 == 0:
        now = time.time()
        elapsed = now - last_time
        rate = 1000 / elapsed

        print(
            f"[Consumer] Recebidas 1000 mensagens em {elapsed:.4f}s | "
            f"Taxa: {rate:.2f} msg/s"
        )

        batch_elapsed.append(elapsed)
        batch_rates.append(rate)

        last_time = now

    # parar após 1 milhão
    if received_count >= NUM_MSGS:
        break

total_time = time.time() - start_time
print("\n====== MÉTRICAS DO CONSUMER ======")
print(f"Total de mensagens recebidas: {received_count}")
print(f"Tempo total: {total_time:.4f} segundos")
print(f"Taxa média: {received_count / total_time:.2f} msg/s")

# === CSV FINAL: salvar todas as métricas ===
with open(CSV_METRICS, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["batch_index", "elapsed_seconds", "rate_msg_per_s"])
    
    for idx, (elapsed, rate) in enumerate(zip(batch_elapsed, batch_rates), start=1):
        writer.writerow([idx, elapsed, rate])

print(f"Métricas salvas em: {CSV_METRICS}")

# === GRÁFICO 1 ===
plt.figure(figsize=(10,5))
plt.plot(batch_rates)
plt.title("Taxa de recebimento por batch de 1000 mensagens")
plt.xlabel("Batch (1000 msgs)")
plt.ylabel("msg/s")
plt.grid()
plt.show()

# === GRÁFICO 2 ===
plt.figure(figsize=(10,5))
plt.plot(batch_elapsed)
plt.title("Tempo por batch de 1000 mensagens")
plt.xlabel("Batch (1000 msgs)")
plt.ylabel("Segundos")
plt.grid()
plt.show()
