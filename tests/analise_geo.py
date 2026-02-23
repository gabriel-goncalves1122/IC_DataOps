import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import os

def carregar_dados():
    print("⏳ Carregando banco geoespacial (Aguarde, a malha de 5.570 municípios é pesada)...")
    dados = {}
    try:
        dados['linhas'] = gpd.read_file("data/geo/linhas_transmissao_ons.geojson")
        dados['subs'] = gpd.read_file("data/geo/subestacoes_ons.geojson")
        dados['iso'] = gpd.read_file("data/geo/sistemas_isolados_ons.geojson")
        dados['tis'] = gpd.read_file("data/geo/terras_indigenas.geojson")
        dados['municipios'] = gpd.read_file("data/geo/municipios_br.geojson")
        print("✅ Dados carregados com sucesso!")
        return dados
    except Exception as e:
        print(f"❌ Erro ao carregar arquivos. Verifique a pasta data/geo/. Erro: {e}")
        return None

def ver_eletrica(dados):
    print("\n⚡ Gerando Mapa da Infraestrutura Elétrica...")
    fig, ax = plt.subplots(figsize=(12, 12))
    
    # Usamos os municípios como um fundo bem claro para dar contexto geográfico
    if not dados['municipios'].empty:
        dados['municipios'].plot(ax=ax, color='whitesmoke', edgecolor='lightgray', linewidth=0.3, alpha=0.5)
        
    if not dados['linhas'].empty:
        dados['linhas'].plot(ax=ax, color='gray', linewidth=1.2, alpha=0.8)
    if not dados['subs'].empty:
        dados['subs'].plot(ax=ax, color='red', markersize=20, marker='^', edgecolor='black', linewidth=0.5)
    if not dados['iso'].empty:
        dados['iso'].plot(ax=ax, color='dodgerblue', markersize=50, marker='*', edgecolor='black', linewidth=0.5)
        
    ax.set_title("Sistema Interligado Nacional (Linhas, Subestações e Isolados)", fontsize=16, fontweight='bold')
    
    leg_linhas = mlines.Line2D([], [], color='gray', linewidth=2, label='Linhas de Transmissão')
    leg_subs = mlines.Line2D([], [], color='red', marker='^', linestyle='None', markersize=8, label='Subestações')
    leg_iso = mlines.Line2D([], [], color='dodgerblue', marker='*', linestyle='None', markersize=12, label='Sistemas Isolados')
    ax.legend(handles=[leg_linhas, leg_subs, leg_iso], loc='lower right', fontsize=12, framealpha=0.9)
    mostrar_mapa(ax)

def ver_indigenas(dados):
    print("\n🌳 Gerando Mapa das Terras Indígenas...")
    fig, ax = plt.subplots(figsize=(12, 12))
    
    if not dados['municipios'].empty:
        dados['municipios'].plot(ax=ax, color='whitesmoke', edgecolor='lightgray', linewidth=0.3, alpha=0.5)
        
    if not dados['tis'].empty:
        dados['tis'].plot(ax=ax, color='mediumseagreen', edgecolor='darkgreen', linewidth=0.8, alpha=0.7)
        
    ax.set_title("Áreas de Proteção: Terras Indígenas (FUNAI)", fontsize=16, fontweight='bold')
    leg_tis = mpatches.Patch(color='mediumseagreen', alpha=0.7, label='Terras Indígenas')
    ax.legend(handles=[leg_tis], loc='lower right', fontsize=12, framealpha=0.9)
    mostrar_mapa(ax)

def ver_municipios(dados):
    print("\n🏛️ Gerando Mapa da Divisão Administrativa...")
    fig, ax = plt.subplots(figsize=(12, 12))
    
    if not dados['municipios'].empty:
        dados['municipios'].plot(ax=ax, color='cornsilk', edgecolor='darkgray', linewidth=0.3)
        
    ax.set_title("Divisão Municipal Brasileira", fontsize=16, fontweight='bold')
    mostrar_mapa(ax)

def ver_tudo(dados):
    print("\n🌍 Gerando o Mapa Completo (Nível de Estresse de Processamento Alto)...")
    fig, ax = plt.subplots(figsize=(12, 12))
    
    # A ordem das camadas aqui é crucial (do fundo para a frente)
    if not dados['municipios'].empty:
        dados['municipios'].plot(ax=ax, color='whitesmoke', edgecolor='lightgray', linewidth=0.2, alpha=0.5)
    if not dados['tis'].empty:
        dados['tis'].plot(ax=ax, color='mediumseagreen', edgecolor='darkgreen', linewidth=0.5, alpha=0.4)
    if not dados['linhas'].empty:
        dados['linhas'].plot(ax=ax, color='dimgray', linewidth=1.2, alpha=0.8)
    if not dados['subs'].empty:
        dados['subs'].plot(ax=ax, color='red', markersize=20, marker='^', edgecolor='black', linewidth=0.5)
    if not dados['iso'].empty:
        dados['iso'].plot(ax=ax, color='dodgerblue', markersize=50, marker='*', edgecolor='black', linewidth=0.5)
        
    ax.set_title("Panorama DataOps Definitivo: Municípios, Energia e Áreas de Proteção", fontsize=16, fontweight='bold')
    
    leg_tis = mpatches.Patch(color='mediumseagreen', alpha=0.5, label='Terras Indígenas')
    leg_linhas = mlines.Line2D([], [], color='dimgray', linewidth=2, label='Linhas de Transmissão')
    leg_subs = mlines.Line2D([], [], color='red', marker='^', linestyle='None', markersize=8, label='Subestações')
    leg_iso = mlines.Line2D([], [], color='dodgerblue', marker='*', linestyle='None', markersize=12, label='Sistemas Isolados')
    ax.legend(handles=[leg_tis, leg_linhas, leg_subs, leg_iso], loc='lower right', fontsize=12, framealpha=0.9)
    mostrar_mapa(ax)

def mostrar_mapa(ax):
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()

def menu():
    os.system('clear' if os.name == 'posix' else 'cls')
    print("="*50)
    print(" 🛰️  ANALISADOR GEOESPACIAL DO DATAOPS (v2.0) 🛰️")
    print("="*50)
    
    dados = carregar_dados()
    if not dados:
        return

    while True:
        print("\nEscolha a visualização desejada:")
        print("  [ 1 ] - Ver SOMENTE Infraestrutura Elétrica")
        print("  [ 2 ] - Ver SOMENTE Terras Indígenas")
        print("  [ 3 ] - Ver SOMENTE Municípios")
        print("  [ 4 ] - Ver TUDO (Cruzamento Completo)")
        print("  [ 0 ] - Sair")
        
        opcao = input("\n👉 Digite o número da opção: ")
        
        if opcao == '1':
            ver_eletrica(dados)
        elif opcao == '2':
            ver_indigenas(dados)
        elif opcao == '3':
            ver_municipios(dados)
        elif opcao == '4':
            ver_tudo(dados)
        elif opcao == '0':
            print("Encerrando analisador. Até logo!")
            break
        else:
            print("❌ Opção inválida.")

if __name__ == "__main__":
    menu()