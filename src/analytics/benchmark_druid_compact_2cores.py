import requests
import time
import pandas as pd
import os
import statistics

DRUID_SQL_URL = "http://localhost:8888/druid/v2/sql"

# Mapeamento com Nomes Refletindo o Novo Estado: Compactado por Ano + 2 Núcleos
QUERIES = {
    "Bench_1_Serie_Temporal_CompactYear_2Cores": """
        SELECT TIME_FLOOR(__time, 'PT1H') AS janela_hora, SUM(total_focos) AS volume_focos_hora, 
               MAX(frp_maximo) AS pico_calor_registrado, MIN(dist_linha_minima_km) AS aproximacao_maxima_torres
        FROM "queimadas_focos_calor" GROUP BY 1 ORDER BY 1 DESC LIMIT 100
    """,
    "Bench_2_Alta_Cardinalidade_CompactYear_2Cores": """
        SELECT municipio, bioma, SUM(total_focos) AS total_focos
        FROM "queimadas_focos_calor" GROUP BY 1, 2 ORDER BY total_focos DESC LIMIT 50
    """,
    "Bench_3_Filtros_Complexos_CompactYear_2Cores": """
        SELECT satelite, COUNT(*) as fatias_de_minuto_processadas, SUM(total_focos) as focos_reais
        FROM "queimadas_focos_calor" WHERE risco_eletrico = 'ALTO' AND bioma = 'Amazônia' AND em_terra_indigena = 'true' GROUP BY 1
    """,
    "BI_1_ONS_Infraestrutura_CompactYear_2Cores": """
        SELECT linha_afetada, estado, SUM(total_focos) as ocorrencias_criticas, 
               MIN(dist_linha_minima_km) as menor_distancia_km, MAX(frp_maximo) as intensidade_maxima_fogo
        FROM "queimadas_focos_calor" WHERE risco_eletrico = 'ALTO' AND linha_afetada <> 'N/A' GROUP BY 1, 2 ORDER BY ocorrencias_criticas DESC LIMIT 15
    """,
    "BI_2_FUNAI_Terras_Indigenas_CompactYear_2Cores": """
        SELECT nome_ti, estado, bioma, SUM(total_focos) as focos_registrados, MAX(frp_maximo) as pico_energia_frp
        FROM "queimadas_focos_calor" WHERE em_terra_indigena = 'true' AND nome_ti <> 'Nenhuma' GROUP BY 1, 2, 3 ORDER BY pico_energia_frp DESC LIMIT 10
    """,
    "BI_3_Defesa_Civil_Risco_CompactYear_2Cores": """
        SELECT estado, risco_eletrico, SUM(total_focos) as volume_focos, MIN(dist_subestacao_minima_km) as menor_dist_subestacao_estado
        FROM "queimadas_focos_calor" GROUP BY 1, 2 ORDER BY volume_focos DESC
    """
}

def run_query(query_str, use_cache=False):
    payload = {"query": query_str, "resultFormat": "object", "context": {"useCache": use_cache, "populateCache": use_cache}}
    start_time = time.time()
    response = requests.post(DRUID_SQL_URL, json=payload, headers={"Content-Type": "application/json"})
    return (time.time() - start_time) * 1000, response.json() if response.status_code == 200 else []

def main():
    print("="*80)
    print(" ⚡ BENCHMARK FINAL (FASE 1): DADOS COMPACTADOS (ANO) + 2 NÚCLEOS ⚡")
    print("="*80)
    
    output_dir = "data/results/tests"
    os.makedirs(output_dir, exist_ok=True)
    metrics = []
    RODADAS = 7

    for name, query in QUERIES.items():
        print(f"\n🔍 Executando: {name}")

        print(f"   ❄️  Executando {RODADAS} rodadas COLD START...")
        cold_times = [run_query(query, False)[0] for _ in range(RODADAS)]
        avg_cold = statistics.mean(cold_times)

        run_query(query, True) # Popula cache
        
        print(f"   🔥 Executando {RODADAS} rodadas WARM START...")
        warm_times = []
        for _ in range(RODADAS):
            t, dados = run_query(query, True)
            warm_times.append(t)
            time.sleep(0.1)

        avg_warm = statistics.mean(warm_times)
        
        if dados: pd.DataFrame(dados).to_csv(f"{output_dir}/{name}.csv", index=False)
        ganho = round(((avg_cold - avg_warm) / avg_cold) * 100, 2) if avg_cold > 0 else 0

        metrics.append({
            "Nome_Consulta": name, "Cold_1_ms": round(cold_times[0], 2), "Cold_2_ms": round(cold_times[1], 2), "Cold_3_ms": round(cold_times[2], 2),
            "Cold_4_ms": round(cold_times[3], 2), "Cold_5_ms": round(cold_times[4], 2), "Cold_6_ms": round(cold_times[5], 2), "Cold_7_ms": round(cold_times[6], 2),
            "Media_Cold_ms": round(avg_cold, 2), "Warm_1_ms": round(warm_times[0], 2), "Warm_2_ms": round(warm_times[1], 2), "Warm_3_ms": round(warm_times[2], 2),
            "Warm_4_ms": round(warm_times[3], 2), "Warm_5_ms": round(warm_times[4], 2), "Warm_6_ms": round(warm_times[5], 2), "Warm_7_ms": round(warm_times[6], 2),
            "Media_Warm_ms": round(avg_warm, 2), "Linhas_Retornadas": len(dados) if dados else 0, "Ganho_Cache_%": ganho
        })
        print(f"   📊 Média Cold: {avg_cold:.2f} ms | Média Warm: {avg_warm:.2f} ms | 🚀 Cache: {ganho}%")

    df_metrics = pd.DataFrame(metrics)
    metric_file = f"{output_dir}/relatorio_estatistico_CompactYear_2Cores.csv"
    df_metrics.to_csv(metric_file, index=False)
    print(f"\n✅ Concluído! Salvo em: {metric_file}")

if __name__ == "__main__":
    main()