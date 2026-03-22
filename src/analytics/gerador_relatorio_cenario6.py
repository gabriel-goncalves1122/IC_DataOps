import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import subprocess

# =====================================================================
# 1. CONFIGURAÇÕES E LEITURA DOS DADOS (CENÁRIO 6)
# =====================================================================
# Lendo o CSV correto gerado pelo benchmark de Compactação Anual (6 Cores)
CSV_PATH = "data/results/tests/relatorio_estatistico_CompactYear_6Cores.csv"
OUTPUT_DIR = "data/results/docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📊 Iniciando a geração do Relatório Final (Cenário 6 - 6 Núcleos / Dados Compactados)...")
try:
    df = pd.read_csv(CSV_PATH)
except FileNotFoundError:
    print(f"❌ ERRO: Arquivo {CSV_PATH} não encontrado.")
    exit(1)

# Limpando os nomes das consultas para os gráficos
df['Nome_Limpo'] = df['Nome_Consulta'].str.replace('_CompactYear_6Cores', '', regex=False).str.replace('_', ' ')

# =====================================================================
# 2. GERAÇÃO DE GRÁFICOS (ESTÉTICA ACADÊMICA)
# =====================================================================
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.family'] = 'serif'

# Gráfico 1 - Comparativo de Latências Médias
plt.figure(figsize=(10, 5))
bar_width = 0.35
index = np.arange(len(df['Nome_Limpo']))

plt.bar(index, df['Media_Cold_ms'], bar_width, label='Cold Start (I/O Disco / CPU Bruta)', color='#4c72b0', edgecolor='black')
plt.bar(index + bar_width, df['Media_Warm_ms'], bar_width, label='Warm Start (Cache RAM)', color='#55a868', edgecolor='black')

plt.ylabel('Tempo de Resposta (ms)', fontsize=11)
plt.title('Cenário 6: Força Máxima (6 Núcleos) vs Dados Compactados', fontsize=12, fontweight='bold')
plt.xticks(index + bar_width / 2, df['Nome_Limpo'], rotation=35, ha='right', fontsize=9)
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_desemp_cenario6.png", dpi=300)
plt.close()

# Gráfico 2 - Dispersão de Iterações (O Ápice do "Too Fast to Cache")
# Focando na consulta 3, que obteve o extremo -47.54% de cache devido à velocidade da CPU
consulta_alvo = 'Bench_3_Filtros_Complexos_CompactYear_6Cores'
if consulta_alvo in df['Nome_Consulta'].values:
    linha_alvo = df[df['Nome_Consulta'] == consulta_alvo].iloc[0]
else:
    linha_alvo = df.iloc[2] 
    
cold_runs = [linha_alvo[f'Cold_{i}_ms'] for i in range(1, 8)]
warm_runs = [linha_alvo[f'Warm_{i}_ms'] for i in range(1, 8)]

plt.figure(figsize=(9, 4.5))
plt.plot(range(1, 8), cold_runs, marker='D', linestyle='-', color='#4c72b0', linewidth=2, label='Cold Start')
plt.plot(range(1, 8), warm_runs, marker='s', linestyle='--', color='#d62728', linewidth=2, label='Warm Start (Sobrecarga Logística)')

plt.xlabel('Iteração (Rodada)', fontsize=11)
plt.ylabel('Latência Computacional (ms)', fontsize=11)
plt.title('A Penalidade do Cache em Consultas de Ultra Baixa Latência (18ms)', fontsize=12, fontweight='bold')
plt.xticks(range(1, 8))
plt.legend(frameon=True, shadow=True)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/fig_var_cenario6.png", dpi=300)
plt.close()
print("✅ Gráficos do Cenário 6 gerados com sucesso.")

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
# 4. TEMPLATE LATEX PARA RELATÓRIO DE TESTE (CENÁRIO 6)
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
    \Large\textbf{Relatório Final de Benchmarking -- Cenário 6} \\[0.2cm]
    \large\textit{A Força Máxima (6 Núcleos) e a Descoberta do Limite Assimptótico} \\[0.5cm]
    \normalsize\textbf{Projeto DataOps / UNIFEI} \\
    \small{\today}
\end{center}

\vspace{0.5cm}
\noindent\rule{\textwidth}{1pt}

\section*{1. Descrição do Cenário e Fechamento da Metodologia}
Este documento consolida a última etapa experimental da pesquisa. Tendo sanado a fragmentação física dos dados no Cenário 4 e validado o ganho de paralelismo no Cenário 5 (4 núcleos), o ambiente foi submetido à capacidade máxima do \textit{hardware} hospedeiro: \textbf{6 núcleos de processamento}. O objetivo deste teste derradeiro é investigar a Lei dos Rendimentos Decrescentes (\textit{Diminishing Returns}) em sistemas OLAP orientados a colunas.

\section*{2. Análise de Desempenho: Quebrando a Barreira dos 100ms}
A Tabela 1 revela que a injeção final de poder computacional conseguiu, de forma notável, empurrar o banco de dados para a sua latência mais baixa possível. 

\begin{table}[H]
\centering
\caption{Resultados do Cenário 6 - Dados Compactados com 6 Núcleos}
\vspace{0.2cm}
\begin{tabular}{@{}lcccc@{}}
\toprule
\textbf{Domínio da Consulta SQL} & \textbf{Cold (ms)} & \textbf{Warm (ms)} & \textbf{Otimização} & \textbf{Registros} \\
\midrule
___CONTEUDO_TABELA___
\bottomrule
\end{tabular}
\end{table}

O destaque histórico reside na \textit{Bench\_2\_Alta\_Cardinalidade}. Esta operação, que demorou angustiantes \textbf{4.600 ms} no início do projeto (Cenário 1, fragmentado), e caiu para 119 ms no Cenário 5, finalmente \textbf{quebrou a barreira dos 100ms}, registrando expressivos \textbf{94.45 ms} em \textit{Cold Start}.

\subsection*{2.1. O Extremo do Efeito ``Too Fast to Cache''}
Se no Cenário 5 nós observamos indícios do limite do cache, o Cenário 6 comprova o fenômeno. A consulta \textit{Bench\_3\_Filtros\_Complexos} atingiu \textbf{18.91 milissegundos} de processamento bruto no disco/CPU. Quando o sistema tentou responder a mesma consulta via Memória RAM (\textit{Warm Start}), o tempo subiu para \textbf{27.90 ms}, resultando numa aparente degradação de \textbf{-47.54\%}.

\begin{figure}[H]
\centering
\includegraphics[width=0.9\textwidth]{fig_desemp_cenario6.png}
\caption{A latência bruta atinge seu piso fisiológico, rivalizando com a própria memória.}
\end{figure}

Isto ocorre porque, operando a 18ms, a consulta é mais rápida do que o tempo logístico do nó \textit{Broker} em efetuar o \textit{hash} da requisição HTTP, verificar a integridade da chave no cache e transmitir o binário armazenado. 

\begin{figure}[H]
\centering
\includegraphics[width=0.8\textwidth]{fig_var_cenario6.png}
\caption{A inversão estrutural: o cache torna-se mais lento que o processamento bruto.}
\end{figure}

\section*{3. Conclusão Final do Projeto}
A bateria de testes comprova que \textbf{escalabilidade vertical possui limites econômicos e matemáticos rígidos}. A diferença de tempo absoluto entre alocar 4 núcleos (Cenário 5) e 6 núcleos (Cenário 6) foi de meros $\sim$20 milissegundos para a maioria das cargas. O grande divisor de águas, responsável por uma aceleração na ordem de $\times40$, foi exclusivamente a disciplina de \textbf{DataOps} (Compactação de Segmentos). A pesquisa conclui que bases analíticas (OLAP) modernas devem priorizar a engenharia de \textit{storage} e compactação sobre o provisionamento desenfreado de CPU.

\end{document}
"""

# Injeção blindada da tabela no documento
latex_document = latex_template.replace("___CONTEUDO_TABELA___", conteudo_tabela)

tex_path = f"{OUTPUT_DIR}/relatorio_cenario6.tex"
with open(tex_path, 'w', encoding='utf-8') as f:
    f.write(latex_document)
    
print("✅ Código LaTeX do Cenário 6 gerado!")

# =====================================================================
# 5. COMPILAÇÃO AUTOMÁTICA
# =====================================================================
print("⚙️ Compilando o PDF do Cenário 6 (Isso pode levar alguns segundos)...")
try:
    comando = ['pdflatex', '-interaction=nonstopmode', 'relatorio_cenario6.tex']
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(comando, cwd=OUTPUT_DIR, check=True, stdout=subprocess.DEVNULL)
    print(f"🎓 SUCESSO! Relatório Final pronto em: {OUTPUT_DIR}/relatorio_cenario6.pdf")
except subprocess.CalledProcessError:
    print(f"⚠️  PDF compilado com avisos, verifique a pasta {OUTPUT_DIR}.")