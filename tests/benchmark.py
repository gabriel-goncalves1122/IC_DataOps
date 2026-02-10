import requests
import time
import json
import statistics
import pandas as pd
from datetime import datetime

# --- CONFIGURAÇÕES ---
DRUID_BROKER_URL = "http://localhost:8082/druid/v2/sql"
RUNS_PER_QUERY = 10  # Quantas vezes repetir cada query para ter relevância estatística

# --- AS QUERIES (SEPARADAS POR COMPLEXIDADE) ---
QUERIES = {
    "1. KPI Geral (Leve)": {
        "sql": """SELECT COUNT(*) AS total, AVG(frp) FROM "inpe_focos_calor" """
    },
    "2. Ranking Biomas (Agregação Média)": {
        "sql": """SELECT bioma, SUM(frp) FROM "inpe_focos_calor" GROUP BY 1 ORDER BY 2 DESC"""
    },
    "3. Série Temporal (Full Scan)": {
        "sql": """SELECT TIME_FLOOR(__time, 'P1D'), COUNT(*) FROM "inpe_focos_calor" GROUP BY 1"""
    },
    "4. Stress Test (Alta Cardinalidade)": {
        "sql": """SELECT municipio, TIME_FLOOR(__time, 'PT1H'), COUNT(*) FROM "inpe_focos_calor" GROUP BY 1, 2 LIMIT 1000"""
    }
}

def execute_query(name, sql, use_cache=False):
    """
    Envia a query para o Druid controlando as Flags de Cache no Payload JSON
    """
    payload = {
        "query": sql,
        "context": {
            # O SEGREDO ESTÁ AQUI:
            "useCache": use_cache,
            "populateCache": use_cache,
            "useResultLevelCache": use_cache
        }
    }
    
    headers = {'Content-Type': 'application/json'}
    
    start_time = time.perf_counter() # Cronômetro de alta precisão
    try:
        response = requests.post(DRUID_BROKER_URL, json=payload, headers=headers)
        response.raise_for_status()
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        # Validação simples para garantir que retornou dados
        rows = len(response.json())
        return duration_ms, rows, None
    except Exception as e:
        return 0, 0, str(e)

def run_benchmark():
    print(f"🚀 INICIANDO BENCHMARK CIENTÍFICO DRUID")
    print(f"📅 Data: {datetime.now().isoformat()}")
    print(f"🔄 Repetições por query: {RUNS_PER_QUERY}")
    print("-" * 80)

    results_data = []

    for q_name, q_data in QUERIES.items():
        print(f"\n🔎 Testando: {q_name}")
        
        # --- BATERIA 1: COLD CACHE (Sem Memória) ---
        cold_times = []
        for i in range(RUNS_PER_QUERY):
            ms, rows, err = execute_query(q_name, q_data['sql'], use_cache=False)
            if err:
                print(f"❌ Erro: {err}")
                break
            cold_times.append(ms)
            print(f"   🧊 Cold Run {i+1}: {ms:.2f}ms ({rows} linhas)")
            time.sleep(0.2) # Pausa leve para não engasgar a rede

        # --- BATERIA 2: WARM CACHE (Com Memória) ---
        # Primeiro fazemos uma execução 'esquenta' para garantir que está na RAM
        execute_query(q_name, q_data['sql'], use_cache=True)
        
        warm_times = []
        for i in range(RUNS_PER_QUERY):
            ms, rows, err = execute_query(q_name, q_data['sql'], use_cache=True)
            warm_times.append(ms)
            print(f"   🔥 Warm Run {i+1}: {ms:.2f}ms")

        # --- ESTATÍSTICAS ---
        if cold_times and warm_times:
            results_data.append({
                "Query": q_name,
                "Cold Avg (ms)": round(statistics.mean(cold_times), 2),
                "Cold P95 (ms)": round(statistics.quantiles(cold_times, n=20)[-1], 2), # 95th Percentile
                "Warm Avg (ms)": round(statistics.mean(warm_times), 2),
                "Speedup (x)": round(statistics.mean(cold_times) / statistics.mean(warm_times), 2)
            })

    # --- RELATÓRIO FINAL ---
    print("\n" + "="*80)
    print("📊 RESULTADO FINAL DO BENCHMARK")
    print("="*80)
    df = pd.DataFrame(results_data)
    # Formata para tabela bonita no terminal
    print(df.to_markdown(index=False))
    
    # Salva para você colocar no relatório
    df.to_csv("benchmark_resultados.csv", index=False)
    print("\n✅ Resultados salvos em 'benchmark_resultados.csv'")

if __name__ == "__main__":
    run_benchmark()