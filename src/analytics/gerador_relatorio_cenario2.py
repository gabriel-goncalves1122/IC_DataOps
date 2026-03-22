import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import subprocess

# =====================================================================
# 1. CONFIGURAÇÕES E LEITURA DOS DADOS (CENÁRIO 2)
# =====================================================================
# Apontando para o CSV de 4 Núcleos
CSV_PATH = "data/results/tests/relatorio_estatistico_6518Seg_4Cores.csv"
OUTPUT_DIR = "data/results/docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📊 Iniciando a geração do Relatório Intermediário (Cenário 2 - 4 Núcleos / Fragmentado)...")
df = pd.read_csv(CSV_PATH)

# Limpando os nomes das consultas para os gráficos
df['Nome_Limpo'] = df['Nome_Consulta'].str.replace('_6518Seg_4Cores', '', regex=False).str.replace('_', ' ')

# =====================================================================
# 2. GERAÇÃO DE GRÁFICOS
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
plt.title('Cenário 2: Escalonamento Vertical (4 Núcleos) e Disco Fragmentado', fontsize=12, fontweight='bold')
plt.xticks(index + bar_width / 2, df['Nome_Limpo'], rotation=35, ha='right', fontsize=9)
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_desemp_cenario2.png", dpi=300)
plt.close()

# Gráfico 2 - Dispersão de Iterações (O Paradoxo do Cache)
consulta_alvo = 'Bench_2_Alta_Cardinalidade_6518Seg_4Cores'
if consulta_alvo in df['Nome_Consulta'].values:
    linha_alvo = df[df['Nome_Consulta'] == consulta_alvo].iloc[0]
else:
    linha_alvo = df.iloc[1]
    
cold_runs = [linha_alvo[f'Cold_{i}_ms'] for i in range(1, 8)]
warm_runs = [linha_alvo[f'Warm_{i}_ms'] for i in range(1, 8)]

plt.figure(figsize=(9, 4.5))
plt.plot(range(1, 8), cold_runs, marker='D', linestyle='-', color='#4c72b0', linewidth=2, label='Cold Start')
plt.plot(range(1, 8), warm_runs, marker='s', linestyle='--', color='#55a868', linewidth=2, label='Warm Start')

plt.xlabel('Iteração (Rodada)', fontsize=11)
plt.ylabel('Latência Computacional (ms)', fontsize=11)
plt.title('Dispersão de Latência: A Persistência do Paradoxo com 4 Núcleos', fontsize=12, fontweight='bold')
plt.xticks(range(1, 8))
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_var_cenario2.png", dpi=300)
plt.close()
print("✅ Gráficos do Cenário 2 gerados com sucesso.")

# =====================================================================
# 3. CONSTRUÇÃO DA TABELA
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
# 4. TEMPLATE LATEX PARA RELATÓRIO (CENÁRIO 2)
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
    \Large\textbf{Relatório Intermediário de Benchmarking -- Cenário 2} \\[0.2cm]
    \large\textit{Avaliação de Desempenho: 4 Núcleos e Armazenamento Fragmentado} \\[0.5cm]
    \normalsize\textbf{Projeto DataOps / UNIFEI} \\
    \small{\today}
\end{center}

\vspace{0.5cm}
\noindent\rule{\textwidth}{1pt}

\section*{1. Descrição do Cenário}
Este documento retrata a segunda fase dos testes de escalabilidade vertical do ecossistema Apache Druid. Comprovado o estrangulamento sistêmico no Cenário 1 (onde apenas 2 núcleos geraram latências de 4.600 ms), o \textit{cluster} foi reconfigurado via \textit{Docker Compose Override}. Alocaram-se \textbf{4 núcleos lógicos totais} (2 \textit{threads} distribuídas para o nó \textit{Broker} e 2 para o nó \textit{Historical}), dobrando assim o poder de fogo paralelizado.

Nesta etapa, o objetivo central foi avaliar o impacto puro do incremento de CPU, mantendo-se inalterada a patologia de armazenamento: os $\sim$211 MB de metadados permanecem distribuídos em \textbf{6.518 minúsculos arquivos (Tiny Segments)}.

\section*{2. Ganhos de Escala vs. O Paradoxo Contínuo}
Os resultados expressos na Tabela 1 demonstram a eficácia da natureza multithread do banco colunar. O aumento de CPU praticamente cortou pela metade a ineficiência de varredura. Consultas complexas que orbitavam em $\sim$4.600 ms despencaram para o patamar seguro de \textbf{2.000 ms a 2.900 ms} em leituras \textit{Cold Start}.

\begin{table}[H]
\centering
\caption{Resultados do Cenário 2 - CPU Intermediária (4 Cores)}
\vspace{0.2cm}
\begin{tabular}{@{}lcccc@{}}
\toprule
\textbf{Domínio da Consulta SQL} & \textbf{Cold (ms)} & \textbf{Warm (ms)} & \textbf{Otimização} & \textbf{Registros} \\
\midrule
___CONTEUDO_TABELA___
\bottomrule
\end{tabular}
\end{table}

Contudo, a constatação científica de maior peso reside na \textbf{ineficiência persistente do Cache}. Apesar da melhoria nos tempos globais, a consulta ``BI\_2 FUNAI'' ainda assinalou otimização negativa (-1.42\%), ou seja, o \textit{Warm Start} demorou mais que o acesso direto aos discos. Isto comprova que injetar processador ameniza a latência geral, mas \textbf{não soluciona o I/O Overhead} (custo de roteamento e fusão em RAM) decorrente de 6.518 blocos estilhaçados.

\begin{figure}[H]
\centering
\includegraphics[width=0.9\textwidth]{fig_desemp_cenario2.png}
\caption{Comparativo atestando ganhos escalares, mas evidenciando falhas de Warm Start.}
\end{figure}

\begin{figure}[H]
\centering
\includegraphics[width=0.8\textwidth]{fig_var_cenario2.png}
\caption{Dispersão refletindo a disputa da ULA no gerenciamento da base não compactada.}
\end{figure}

\section*{3. Considerações e Próximos Passos}
A duplicação de núcleos validou o princípio de escalabilidade teórica, mas escancarou que motores OLAP são visceralmente dependentes da geometria dos dados. O último teste desta fase arquitetural (Cenário 3) levará a máquina ao limite físico (6 núcleos) antes que se aplique a resolução mandatória de \textit{DataOps}: a Compactação dos Segmentos.

\end{document}
"""

# Injeção blindada
latex_document = latex_template.replace("___CONTEUDO_TABELA___", conteudo_tabela)

tex_path = f"{OUTPUT_DIR}/relatorio_cenario2.tex"
with open(tex_path, 'w', encoding='utf-8') as f:
    f.write(latex_document)
    
print("✅ Código LaTeX do Cenário 2 gerado!")

# =====================================================================
# 5. COMPILAÇÃO AUTOMÁTICA
# =====================================================================
print("⚙️ Compilando o PDF do Cenário 2...")
try:
    comando = ['pdflatex', '-interaction=nonstopmode', 'relatorio_cenario2.tex']
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    print(f"🎓 SUCESSO! Relatório Intermediário 2 pronto em: {OUTPUT_DIR}/relatorio_cenario2.pdf")
except subprocess.CalledProcessError:
    print(f"⚠️  PDF compilado com avisos, verifique a pasta {OUTPUT_DIR}.")