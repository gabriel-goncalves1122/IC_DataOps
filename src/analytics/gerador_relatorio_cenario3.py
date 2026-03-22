import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import subprocess

# =====================================================================
# 1. CONFIGURAÇÕES E LEITURA DOS DADOS (CENÁRIO 3)
# =====================================================================
# Lendo exclusivamente o CSV do Cenário 3 (6 Cores / 6518 Segmentos)
CSV_PATH = "data/results/tests/relatorio_estatistico_6518Seg_6Cores.csv"
OUTPUT_DIR = "data/results/docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📊 Gerando Relatório Intermediário (Cenário 3 - Análise Sequencial e Colapso do Cache)...")
df = pd.read_csv(CSV_PATH)

# Limpando os nomes das consultas para os gráficos
df['Nome_Limpo'] = df['Nome_Consulta'].str.replace('_6518Seg_6Cores', '', regex=False).str.replace('_', ' ')

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
plt.title('Cenário 3: Latência com 6 Núcleos e Armazenamento Fragmentado', fontsize=12, fontweight='bold')
plt.xticks(index + bar_width / 2, df['Nome_Limpo'], rotation=35, ha='right', fontsize=9)
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_desemp_cenario3.png", dpi=300)
plt.close()

# Gráfico 2 - Dispersão de Iterações (O Ápice do Paradoxo do Cache)
# Selecionando a consulta que teve o pior rendimento de RAM (-12.47%)
consulta_alvo = 'BI_2_FUNAI_Terras_Indigenas_6518Seg_6Cores'
if consulta_alvo in df['Nome_Consulta'].values:
    linha_alvo = df[df['Nome_Consulta'] == consulta_alvo].iloc[0]
else:
    linha_alvo = df.iloc[4]
    
cold_runs = [linha_alvo[f'Cold_{i}_ms'] for i in range(1, 8)]
warm_runs = [linha_alvo[f'Warm_{i}_ms'] for i in range(1, 8)]

plt.figure(figsize=(9, 4.5))
plt.plot(range(1, 8), cold_runs, marker='D', linestyle='-', color='#4c72b0', linewidth=2, label='Cold Start')
plt.plot(range(1, 8), warm_runs, marker='s', linestyle='--', color='#55a868', linewidth=2, label='Warm Start')

plt.xlabel('Iteração (Rodada)', fontsize=11)
plt.ylabel('Latência Computacional (ms)', fontsize=11)
plt.title('Dispersão de Latência: O Colapso do Cache (6 Cores)', fontsize=12, fontweight='bold')
plt.xticks(range(1, 8))
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_var_cenario3.png", dpi=300)
plt.close()
print("✅ Gráficos do Cenário 3 gerados com sucesso.")

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
# 4. TEMPLATE LATEX EXCLUSIVO PARA O CENÁRIO 3
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
    \Large\textbf{Relatório Intermediário de Benchmarking -- Cenário 3} \\[0.2cm]
    \large\textit{Análise Sequencial: Escalonamento Máximo (6 Núcleos) e o Colapso do Cache} \\[0.5cm]
    \normalsize\textbf{Projeto DataOps / UNIFEI} \\
    \small{\today}
\end{center}

\vspace{0.5cm}
\noindent\rule{\textwidth}{1pt}

\section*{1. Descrição do Cenário e Progressão Histórica}
Este documento consolida a terceira e última iteração da fase de testes focada no estrangulamento de \textit{hardware} (escalabilidade vertical). Nos relatórios precedentes, a mesmíssima base de dados fragmentada (3,1 milhões de registros distribuídos em 6.518 arquivos minúsculos - \textit{Tiny Segments}) foi submetida a processamentos sob rígidas restrições.

Em uma análise sequencial evolutiva, constatou-se que:
\begin{itemize}
    \item \textbf{No Cenário 1 (2 Núcleos):} O sistema agonizou em severo estrangulamento de \textit{I/O Overhead}, apresentando latências médias cruas na faixa dos $\sim$4.091 ms.
    \item \textbf{No Cenário 2 (4 Núcleos):} O provimento de paralelismo reduziu a ineficiência quase pela metade, estabilizando as leituras na casa dos $\sim$2.319 ms.
\end{itemize}

Neste \textbf{Cenário 3}, as travas do orquestrador Docker (\textit{Cgroups}) foram plenamente liberadas para a capacidade máxima do hospedeiro, alocando-se \textbf{6 núcleos lógicos} (3 \textit{threads} para o \textit{Broker} e 3 para o \textit{Historical}), acompanhados de 100 MB de alocação de \textit{Direct Memory}. O objetivo empírico é verificar se o \textit{hardware} de ponta consegue, por força bruta, superar o gargalo geométrico do disco.

\section*{2. Escalabilidade de Disco vs. O Colapso Absoluto do Cache}
Os resultados expressos na Tabela 1 demonstram a clássica escalabilidade teórica prevista pela Lei de Amdahl. A latência de varredura direta sobre os arquivos (\textit{Cold Start}) apresentou uma nova e expressiva melhoria, atingindo o teto de máxima eficiência desta arquitetura desorganizada com uma média global estabilizada em \textbf{$\sim$1.624 ms}.

\begin{table}[H]
\centering
\caption{Resultados do Cenário 3 - Capacidade Máxima (6 Cores) vs Fragmentação}
\vspace{0.2cm}
\begin{tabular}{@{}lcccc@{}}
\toprule
\textbf{Domínio da Consulta SQL} & \textbf{Cold (ms)} & \textbf{Warm (ms)} & \textbf{Otimização} & \textbf{Registros} \\
\midrule
___CONTEUDO_TABELA___
\bottomrule
\end{tabular}
\end{table}

Contudo, a grande descoberta analítica desta etapa reside na memória RAM. A Tabela revela que o aumento extremo de processamento simultâneo \textbf{agravou severamente o Paradoxo do Cache}. Das seis consultas estratégicas disparadas, cinco performaram substancialmente \textbf{pior} no \textit{Warm Start} do que no acesso cru ao disco. O painel da FUNAI (\textit{BI\_2}) alcançou absurdos \textbf{-12,47\% de degradação estrutural}.

\begin{figure}[H]
\centering
\includegraphics[width=0.9\textwidth]{fig_desemp_cenario3.png}
\caption{O recuo das latências brutas em contraposição ao congestionamento do cache.}
\end{figure}

\begin{figure}[H]
\centering
\includegraphics[width=0.8\textwidth]{fig_var_cenario3.png}
\caption{O exato momento (Consulta BI\_2) em que ler do disco torna-se mais vantajoso que o cache.}
\end{figure}

Isto fundamenta o fato de que, ao interceptar as \textit{queries}, o nó \textit{Broker} tenta varrer, unificar e agrupar na Memória Volátil as parciais herdadas dos 6.518 micro-arquivos. O custo logístico temporal desse "quebra-cabeça" afoga a ULA, superando amplamente o tempo que os 6 núcleos levariam simplesmente lendo o disco em paralelo.

\section*{3. Veredito da Fase de Escalabilidade}
A análise encadeada dos três cenários valida inequivocamente a premissa de que a escalabilidade puramente atrelada a \textit{hardware} possui um "teto de vidro" intransponível. A injeção de mais núcleos colapsou a estrutura interna de \textit{Cache}. Para romper a barreira técnica dos milissegundos e habilitar o pleno tempo-real (sub-segundos), abandona-se o \textit{Hardware Tuning} em prol do \textit{DataOps}, onde a próxima bateria de testes lidará com os dados plenamente compactados (\textit{Compaction}).

\end{document}
"""

# Injeção blindada da tabela no documento
latex_document = latex_template.replace("___CONTEUDO_TABELA___", conteudo_tabela)

tex_path = f"{OUTPUT_DIR}/relatorio_cenario3.tex"
with open(tex_path, 'w', encoding='utf-8') as f:
    f.write(latex_document)
    
print("✅ Código LaTeX do Cenário 3 gerado!")

# =====================================================================
# 5. COMPILAÇÃO AUTOMÁTICA
# =====================================================================
print("⚙️ Compilando o PDF do Cenário 3 (Isso pode levar alguns segundos)...")
try:
    comando = ['pdflatex', '-interaction=nonstopmode', 'relatorio_cenario3.tex']
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    print(f"🎓 SUCESSO! Relatório Intermediário pronto em: {OUTPUT_DIR}/relatorio_cenario3.pdf")
except subprocess.CalledProcessError:
    print(f"⚠️  PDF compilado com avisos, verifique a pasta {OUTPUT_DIR}.")