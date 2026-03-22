import requests
import time
import pandas as pd
import os
import statistics

# =====================================================================
# CONFIGURAÇÕES DA API REST DO APACHE DRUID
# =====================================================================
DRUID_SQL_URL = "http://localhost:8888/druid/v2/sql"

# Mapeamento com Nomes Refletindo o Estado Atual: 6.518 Segmentos + 6 Núcleos
QUERIES = {
    "Bench_1_Serie_Temporal_6518Seg_6Cores": """
        SELECT 
          TIME_FLOOR(__time, 'PT1H') AS janela_hora, 
          SUM(total_focos) AS volume_focos_hora, 
          MAX(frp_maximo) AS pico_calor_registrado,
          MIN(dist_linha_minima_km) AS aproximacao_maxima_torres
        FROM "queimadas_focos_calor"
        GROUP BY 1 
        ORDER BY 1 DESC
        LIMIT 100
    """,
    "Bench_2_Alta_Cardinalidade_6518Seg_6Cores": """
        SELECT 
          municipio, 
          bioma, 
          SUM(total_focos) AS total_focos
        FROM "queimadas_focos_calor"
        GROUP BY 1, 2 
        ORDER BY total_focos DESC 
        LIMIT 50
    """,
    "Bench_3_Filtros_Complexos_6518Seg_6Cores": """
        SELECT 
          satelite,
          COUNT(*) as fatias_de_minuto_processadas,
          SUM(total_focos) as focos_reais
        FROM "queimadas_focos_calor"
        WHERE risco_eletrico = 'ALTO' 
          AND bioma = 'Amazônia' 
          AND em_terra_indigena = 'true'
        GROUP BY 1
    """,
    "BI_1_ONS_Infraestrutura_6518Seg_6Cores": """
        SELECT 
          linha_afetada, 
          estado,
          SUM(total_focos) as ocorrencias_criticas, 
          MIN(dist_linha_minima_km) as menor_distancia_km, 
          MAX(frp_maximo) as intensidade_maxima_fogo
        FROM "queimadas_focos_calor"
        WHERE risco_eletrico = 'ALTO' AND linha_afetada <> 'N/A'
        GROUP BY 1, 2
        ORDER BY ocorrencias_criticas DESC 
        LIMIT 15
    """,
    "BI_2_FUNAI_Terras_Indigenas_6518Seg_6Cores": """
        SELECT 
          nome_ti, 
          estado, 
          bioma, 
          SUM(total_focos) as focos_registrados, 
          MAX(frp_maximo) as pico_energia_frp
        FROM "queimadas_focos_calor"
        WHERE em_terra_indigena = 'true' AND nome_ti <> 'Nenhuma'
        GROUP BY 1, 2, 3
        ORDER BY pico_energia_frp DESC 
        LIMIT 10
    """,
    "BI_3_Defesa_Civil_Risco_6518Seg_6Cores": """
        SELECT 
          estado, 
          risco_eletrico, 
          SUM(total_focos) as volume_focos,
          MIN(dist_subestacao_minima_km) as menor_dist_subestacao_estado
        FROM "queimadas_focos_calor"
        GROUP BY 1, 2
        ORDER BY volume_focos DESC
    """
}

def run_query(query_str, use_cache=False):
    """Executa a query via REST API controlando o uso do Cache."""
    payload = {
        "query": query_str,
        "resultFormat": "object",
        "context": {
            "useCache": use_cache,
            "populateCache": use_cache
        }
    }
    headers = {"Content-Type": "application/json"}
    
    start_time = time.time()
    response = requests.post(DRUID_SQL_URL, json=payload, headers=headers)
    end_time = time.time()

    elapsed_ms = (end_time - start_time) * 1000

    if response.status_code == 200:
        return elapsed_ms, response.json()
    else:
        print(f"❌ Erro na query: {response.status_code} - {response.text}")
        return elapsed_ms, []

def main():
    print("="*80)
    print(" ⚡ BENCHMARK: DADOS FRAGMENTADOS (6.518 SEGMENTOS) + CPU TUNING (6 NÚCLEOS) ⚡")
    print("="*80)
    
    output_dir = "data/results/tests"
    os.makedirs(output_dir, exist_ok=True)
    metrics = []

    RODADAS = 7

    for name, query in QUERIES.items():
        print(f"\n🔍 Executando: {name}")

        print(f"   ❄️  Executando {RODADAS} rodadas COLD START (Lendo do Disco)...")
        cold_times = []
        for i in range(RODADAS):
            tempo, _ = run_query(query, use_cache=False)
            cold_times.append(tempo)
            time.sleep(0.2)
        
        avg_cold = statistics.mean(cold_times)

        # "Gole de Aquecimento" para popular o cache do Druid
        _, _ = run_query(query, use_cache=True)
        
        print(f"   🔥 Executando {RODADAS} rodadas WARM START (Lendo da Memória RAM)...")
        warm_times = []
        dados_finais = []
        for i in range(RODADAS):
            tempo, dados_finais = run_query(query, use_cache=True)
            warm_times.append(tempo)
            time.sleep(0.2)

        avg_warm = statistics.mean(warm_times)
        linhas_retornadas = len(dados_finais) if dados_finais else 0

        # Exporta o CSV com os dados reais desta consulta com o novo nome
        if dados_finais:
            df_result = pd.DataFrame(dados_finais)
            df_result.to_csv(f"{output_dir}/{name}.csv", index=False)

        ganho_cache = round(((avg_cold - avg_warm) / avg_cold) * 100, 2) if avg_cold > 0 else 0

        metrics.append({
            "Nome_Consulta": name,
            "Cold_1_ms": round(cold_times[0], 2),
            "Cold_2_ms": round(cold_times[1], 2),
            "Cold_3_ms": round(cold_times[2], 2),
            "Cold_4_ms": round(cold_times[3], 2),
            "Cold_5_ms": round(cold_times[4], 2),
            "Cold_6_ms": round(cold_times[5], 2),
            "Cold_7_ms": round(cold_times[6], 2),
            "Media_Cold_ms": round(avg_cold, 2),
            "Warm_1_ms": round(warm_times[0], 2),
            "Warm_2_ms": round(warm_times[1], 2),
            "Warm_3_ms": round(warm_times[2], 2),
            "Warm_4_ms": round(warm_times[3], 2),
            "Warm_5_ms": round(warm_times[4], 2),
            "Warm_6_ms": round(warm_times[5], 2),
            "Warm_7_ms": round(warm_times[6], 2),
            "Media_Warm_ms": round(avg_warm, 2),
            "Linhas_Retornadas": linhas_retornadas,
            "Ganho_Cache_%": ganho_cache
        })

        print(f"   📊 Média Cold: {avg_cold:.2f} ms | Média Warm: {avg_warm:.2f} ms")
        print(f"   🚀 Otimização do Cache: {ganho_cache}%")

    df_metrics = pd.DataFrame(metrics)
    
    # NOME EXCLUSIVO PARA O RELATÓRIO MESTRE DESTE TESTE
    metric_file = f"{output_dir}/relatorio_estatistico_6518Seg_6Cores.csv"
    df_metrics.to_csv(metric_file, index=False)
    
    print("-" * 80)
    print(f"✅ Relatório de testes salvo com sucesso em:")
    print(f"   -> {metric_file}")

if __name__ == "__main__":
    main()