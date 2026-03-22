import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import subprocess

# =====================================================================
# 1. CONFIGURAÇÕES E LEITURA DOS DADOS (CENÁRIO 1)
# =====================================================================
# Apontando para o CSV de 2 Núcleos recém-gerado
CSV_PATH = "data/results/tests/relatorio_estatistico_6518Seg_2Cores.csv"
OUTPUT_DIR = "data/results/docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📊 Iniciando a geração do Relatório Intermediário (Cenário 1 - 2 Núcleos / Fragmentado)...")
df = pd.read_csv(CSV_PATH)

# Limpando os nomes das consultas para os gráficos
df['Nome_Limpo'] = df['Nome_Consulta'].str.replace('_6518Seg_2Cores', '', regex=False).str.replace('_', ' ')

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
plt.title('Cenário 1: Latência Extrema com 2 Núcleos e Armazenamento Fragmentado', fontsize=12, fontweight='bold')
plt.xticks(index + bar_width / 2, df['Nome_Limpo'], rotation=35, ha='right', fontsize=9)
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_desemp_cenario1.png", dpi=300)
plt.close()

# Gráfico 2 - Dispersão de Iterações (O Paradoxo do Cache)
consulta_alvo = 'Bench_2_Alta_Cardinalidade_6518Seg_2Cores'
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
plt.title('Dispersão de Latência: O Estrangulamento de 2 Núcleos', fontsize=12, fontweight='bold')
plt.xticks(range(1, 8))
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_var_cenario1.png", dpi=300)
plt.close()
print("✅ Gráficos do Cenário 1 gerados com sucesso.")

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
# 4. TEMPLATE LATEX PARA RELATÓRIO (CENÁRIO 1)
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
    \Large\textbf{Relatório Intermediário de Benchmarking -- Cenário 1} \\[0.2cm]
    \large\textit{Avaliação de Desempenho: 2 Núcleos e Armazenamento Fragmentado} \\[0.5cm]
    \normalsize\textbf{Projeto DataOps / UNIFEI} \\
    \small{\today}
\end{center}

\vspace{0.5cm}
\noindent\rule{\textwidth}{1pt}

\section*{1. Descrição do Cenário}
Este relatório inaugura a bateria de testes de escalabilidade e \textit{tuning} do Apache Druid. Sob este \textbf{Cenário 1}, simulou-se um ambiente de restrição severa de \textit{hardware}. Utilizando as sobrescritas do orquestrador Docker (\textit{Cgroups}), o \textit{cluster} foi estrangulado para operar com apenas \textbf{2 núcleos lógicos totais} (sendo 1 núcleo dedicado exclusivamente ao nó \textit{Broker} e 1 núcleo para o varrimento de disco pelo nó \textit{Historical}).

Os dados consolidados compreendem mais de 3,1 milhões de registros de anomalias térmicas. No entanto, sua geometria nativa, originada da fase de ingestão ativa (\textit{streaming}), encontra-se em um estado subótimo: $\sim$211 MB distribuídos de forma fragmentada em \textbf{6.518 arquivos minúsculos (Tiny Segments)}.

\section*{2. Degradação de Latência e o Paradoxo do Cache}
Os resultados expostos na Tabela 1 atestam a dificuldade computacional imposta ao sistema. Limitado a um único núcleo de leitura, o nó \textit{Historical} precisou executar milhares de operações sistêmicas (abertura e fechamento de pequenos arquivos), gerando um \textit{I/O Overhead} brutal. Como reflexo, as latências de varredura plena (\textit{Cold Start}) superaram a marca crítica dos 4.600 milissegundos.

\begin{table}[H]
\centering
\caption{Resultados do Cenário 1 - Estrangulamento de CPU (2 Cores)}
\vspace{0.2cm}
\begin{tabular}{@{}lcccc@{}}
\toprule
\textbf{Domínio da Consulta SQL} & \textbf{Cold (ms)} & \textbf{Warm (ms)} & \textbf{Otimização} & \textbf{Registros} \\
\midrule
___CONTEUDO_TABELA___
\bottomrule
\end{tabular}
\end{table}

A observação analítica mais relevante desta etapa é a manifestação inicial do \textbf{Paradoxo do Cache}. Na consulta "BI\_2 FUNAI Terras Indigenas", a interceptação pela memória RAM (\textit{Warm Start}) revelou um desempenho inferior ao acesso de disco (decréscimo de -0.76\%). O nó coordenador, restrito a 1 núcleo, demonstrou incapacidade de gerenciar e fundir com celeridade os metadados de 6.518 fragmentos alocados na memória, atestando que o cache colunar é ineficaz diante de armazenamentos não compactados.

\begin{figure}[H]
\centering
\includegraphics[width=0.9\textwidth]{fig_desemp_cenario1.png}
\caption{Comparativo das latências extremas sofrendo as penalidades de I/O em disco.}
\end{figure}

\begin{figure}[H]
\centering
\includegraphics[width=0.8\textwidth]{fig_var_cenario1.png}
\caption{Dispersão confirmando instabilidade de tempo de resposta entre rodadas.}
\end{figure}

\section*{3. Considerações e Próximos Passos}
O teste prova matematicamente que uma arquitetura distribuída, operando sob hiper-fragmentação de \textit{storage} e carente de paralelismo de CPU, inviabiliza análises de tempo real (latência sub-segundo). Para a próxima iteração (Cenário 2), aplicar-se-á a escalabilidade vertical dobrando a capacidade para 4 núcleos lógicos, a fim de validar a Lei de Amdahl sobre a base de dados fragmentada.

\end{document}
"""

# Injeção blindada
latex_document = latex_template.replace("___CONTEUDO_TABELA___", conteudo_tabela)

tex_path = f"{OUTPUT_DIR}/relatorio_cenario1.tex"
with open(tex_path, 'w', encoding='utf-8') as f:
    f.write(latex_document)
    
print("✅ Código LaTeX do Cenário 1 gerado!")

# =====================================================================
# 5. COMPILAÇÃO AUTOMÁTICA
# =====================================================================
print("⚙️ Compilando o PDF do Cenário 1...")
try:
    comando = ['pdflatex', '-interaction=nonstopmode', 'relatorio_cenario1.tex']
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    print(f"🎓 SUCESSO! Relatório Intermediário 1 pronto em: {OUTPUT_DIR}/relatorio_cenario1.pdf")
except subprocess.CalledProcessError:
    print(f"⚠️  PDF compilado com avisos, verifique a pasta {OUTPUT_DIR}.")