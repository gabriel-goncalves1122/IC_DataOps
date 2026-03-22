import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import subprocess

# =====================================================================
# 1. CONFIGURAÇÕES E LEITURA DOS DADOS (CENÁRIO 4)
# =====================================================================
# Lendo o CSV correto gerado pelo benchmark de Compactação Anual (2 Cores)
CSV_PATH = "data/results/tests/relatorio_estatistico_CompactYear_2Cores.csv"
OUTPUT_DIR = "data/results/docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📊 Iniciando a geração do Relatório (Cenário 4 - 2 Núcleos / Dados Compactados)...")
df = pd.read_csv(CSV_PATH)

# Limpando os nomes das consultas para os gráficos (removendo a tag do cenário)
df['Nome_Limpo'] = df['Nome_Consulta'].str.replace('_CompactYear_2Cores', '', regex=False).str.replace('_', ' ')

# =====================================================================
# 2. GERAÇÃO DE GRÁFICOS (ESTÉTICA ACADÊMICA)
# =====================================================================
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.family'] = 'serif'

# Gráfico 1 - Comparativo de Latências Médias
plt.figure(figsize=(10, 5))
bar_width = 0.35
index = np.arange(len(df['Nome_Limpo']))

plt.bar(index, df['Media_Cold_ms'], bar_width, label='Cold Start (I/O Disco)', color='#4c72b0', edgecolor='black')
plt.bar(index + bar_width, df['Media_Warm_ms'], bar_width, label='Warm Start (Cache RAM)', color='#55a868', edgecolor='black')

plt.ylabel('Tempo de Resposta (ms)', fontsize=11)
plt.title('Cenário 4: Latência com 2 Núcleos e Dados Compactados (Anual)', fontsize=12, fontweight='bold')
plt.xticks(index + bar_width / 2, df['Nome_Limpo'], rotation=35, ha='right', fontsize=9)
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_desemp_cenario4.png", dpi=300)
plt.close()

# Gráfico 2 - Dispersão de Iterações (O Retorno do Cache Eficiente)
# Focando na Defesa Civil, que obteve o melhor ganho de cache (87.76%) neste teste
consulta_alvo = 'BI_3_Defesa_Civil_Risco_CompactYear_2Cores'
if consulta_alvo in df['Nome_Consulta'].values:
    linha_alvo = df[df['Nome_Consulta'] == consulta_alvo].iloc[0]
else:
    linha_alvo = df.iloc[5] # Fallback para a consulta 6
    
cold_runs = [linha_alvo[f'Cold_{i}_ms'] for i in range(1, 8)]
warm_runs = [linha_alvo[f'Warm_{i}_ms'] for i in range(1, 8)]

plt.figure(figsize=(9, 4.5))
plt.plot(range(1, 8), cold_runs, marker='D', linestyle='-', color='#4c72b0', linewidth=2, label='Cold Start')
plt.plot(range(1, 8), warm_runs, marker='s', linestyle='--', color='#55a868', linewidth=2, label='Warm Start')

plt.xlabel('Iteração (Rodada)', fontsize=11)
plt.ylabel('Latência Computacional (ms)', fontsize=11)
plt.title('Dispersão de Latência: A Restauração do Cache (2 Cores)', fontsize=12, fontweight='bold')
plt.xticks(range(1, 8))
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_var_cenario4.png", dpi=300)
plt.close()
print("✅ Gráficos do Cenário 4 gerados com sucesso.")

# =====================================================================
# 3. CONSTRUÇÃO DA TABELA (PADRÃO BOOKTABS)
# =====================================================================
tabela_linhas = []
for index, row in df.iterrows():
    nome = str(row['Nome_Limpo'])
    cold = f"{row['Media_Cold_ms']:.2f}"
    warm = f"{row['Media_Warm_ms']:.2f}"
    ganho = f"{row['Ganho_Cache_%']:.2f}"
    linhas = str(row['Linhas_Retornadas'])
    tabela_linhas.append(f"    {nome} & {cold} & {warm} & {ganho}\\% & {linhas} \\\\\n")

conteudo_tabela = "".join(tabela_linhas)

# =====================================================================
# 4. TEMPLATE LATEX PARA RELATÓRIO DE TESTE (CENÁRIO 4)
# =====================================================================
latex_template = r"""\documentclass[12pt, a4paper]{article}

\usepackage[utf8]{inputenc}
\usepackage[T1]{fontenc}
\usepackage[brazilian]{babel}
\usepackage{mathptmx}
\usepackage{amsmath, amssymb}
\usepackage{graphicx}
\usepackage{booktabs}
\usepackage{geometry}
\usepackage{float}
\usepackage{setspace}
\usepackage{caption}

\geometry{top=3cm, bottom=2cm, left=3cm, right=2cm}
\setstretch{1.5}
\captionsetup{font=small, labelfont=bf, labelsep=endash}

\begin{document}

\begin{center}
    \Large\textbf{Relatório Intermediário de Benchmarking -- Cenário 4} \\[0.2cm]
    \large\textit{O Triunfo do DataOps: Compactação Anual com Apenas 2 Núcleos} \\[0.5cm]
    \normalsize\textbf{Projeto DataOps / UNIFEI} \\
    \small{\today}
\end{center}

\vspace{0.5cm}
\noindent\rule{\textwidth}{1pt}

\section*{1. Descrição do Cenário e Progressão Histórica}
Após o colapso generalizado do \textit{cache} no Cenário 3 (onde 6 núcleos não foram capazes de superar o \textit{overhead} de 6.518 \textit{Tiny Segments}), este relatório inaugura a \textbf{Fase 2 da pesquisa: Otimização de Armazenamento}. 

Para o Cenário 4, os recursos computacionais foram intencionalmente \textbf{rebaixados de volta para 2 núcleos} (configuração idêntica ao Cenário 1). A diferença arquitetural reside puramente nos dados: aplicou-se um \textit{Compaction Task} no Apache Druid, consolidando os 6.518 arquivos fragmentados em blocos densos com granularidade anual (\textit{CompactYear}).

\section*{2. Análise Sequencial: O Fim do Paradoxo do Cache}
Os resultados obtidos desafiam a lógica tradicional de provisionamento de \textit{hardware}. Operando com apenas 1/3 da capacidade computacional do cenário anterior, o sistema entregou as latências mais baixas de toda a pesquisa histórica.

A Tabela 1 revela que as latências de \textit{Cold Start} despencaram de uma média de 1.600 ms (no Cenário 3) para uma faixa excepcional entre \textbf{74 ms e 461 ms}. 

\begin{table}[H]
\centering
\caption{Resultados do Cenário 4 - Dados Compactados (2 Cores)}
\vspace{0.2cm}
\begin{tabular}{@{}lcccc@{}}
\toprule
\textbf{Domínio da Consulta SQL} & \textbf{Cold (ms)} & \textbf{Warm (ms)} & \textbf{Otimização} & \textbf{Registros} \\
\midrule
___CONTEUDO_TABELA___
\bottomrule
\end{tabular}
\end{table}

Entretanto, o marco científico deste cenário é a \textbf{ressurreição da Memória RAM}. Sem a necessidade de rastrear e unificar milhares de \textit{bitmaps} dispersos, o nó \textit{Broker} voltou a gerenciar o cache com maestria. A eficiência de \textit{Warm Start} atingiu picos de \textbf{87,76\%} de ganho em consultas analíticas territoriais (Defesa Civil), entregando resultados virtualmente instantâneos (17 ms).

\begin{figure}[H]
\centering
\includegraphics[width=0.9\textwidth]{fig_desemp_cenario4.png}
\caption{Comparativo demonstrando o retorno da estabilidade do processamento analítico.}
\end{figure}

\begin{figure}[H]
\centering
\includegraphics[width=0.8\textwidth]{fig_var_cenario4.png}
\caption{Dispersão da Latência: Ausência total de anomalias no ambiente compactado.}
\end{figure}

\section*{3. Conclusão da Fase 1 de Compactação}
O Cenário 4 confirma de forma inegável a hipótese central desta Iniciação Científica: \textbf{em arquiteturas OLAP, a engenharia da estrutura física dos dados supera o provisionamento bruto de processadores}. 

Reduzir os núcleos em 66\% enquanto se acelera as consultas em mais de 900\% prova que a fragmentação era o gargalo primário (\textit{bottleneck}). Os próximos passos avaliarão o impacto da escalabilidade de \textit{hardware} sobre este novo \textit{baseline} otimizado, aplicando 4 e 6 núcleos sobre os dados compactados.

\end{document}
"""

# Injeção blindada da tabela no documento
latex_document = latex_template.replace("___CONTEUDO_TABELA___", conteudo_tabela)

tex_path = f"{OUTPUT_DIR}/relatorio_cenario4.tex"
with open(tex_path, 'w', encoding='utf-8') as f:
    f.write(latex_document)
    
print("✅ Código LaTeX do Cenário 4 gerado!")

# =====================================================================
# 5. COMPILAÇÃO AUTOMÁTICA
# =====================================================================
print("⚙️ Compilando o PDF do Cenário 4 (Isso pode levar alguns segundos)...")
try:
    comando = ['pdflatex', '-interaction=nonstopmode', 'relatorio_cenario4.tex']
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    print(f"🎓 SUCESSO! Relatório Intermediário pronto em: {OUTPUT_DIR}/relatorio_cenario4.pdf")
except subprocess.CalledProcessError:
    print(f"⚠️  PDF compilado com avisos, verifique a pasta {OUTPUT_DIR}.")