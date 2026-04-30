import geopandas as gpd
import pandas as pd
import unicodedata
import os
import re

# ==========================================
# 1. Configurações de Diretórios
# ==========================================
GEO_DIR = os.path.join('data', 'geo')
DIM_DIR = os.path.join('data', 'processed', 'dimensions')
os.makedirs(DIM_DIR, exist_ok=True)

def remover_acentos(texto):
    """Limpeza padrão DataOps para garantir os Joins."""
    if pd.isna(texto): return "NAO INFORMADO"
    texto = str(texto).upper().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

print("==================================================")
print("🏗️  PROCESSANDO INFRAESTRUTURA CRÍTICA (ONS E DNIT)")
print("==================================================")

# ==========================================
# 2. Processando Rodovias Federais
# ==========================================
file_rodovias = os.path.join(GEO_DIR, 'mapa_rodovias_federais.geojson')
out_rodovias = os.path.join(DIM_DIR, 'dim_rodovias_federais.csv')

if os.path.exists(file_rodovias):
    print("\nLendo Rodovias Federais...")
    gdf_rod = gpd.read_file(file_rodovias)
    
    # Vamos supor que as colunas brutas sejam parecidas com isso. 
    # (O Geopandas ignora colunas que não chamamos, então pegamos as principais)
    # Ajuste os nomes brutos (vl_br, ds_jurisdi...) conforme o seu geojson real caso de erro
    cols_rename = {
        'vl_br': 'codigo_rodovia',
        'ds_jurisdi': 'jurisdicao',
        'ds_revesti': 'revestimento',
        'ds_tipo_pa': 'tipo_pavimento',
        'ds_situaca': 'situacao_fisica',
        'ds_operaci': 'status_operacional'
    }
    
    # Renomeia o que encontrar
    gdf_rod = gdf_rod.rename(columns={k: v for k, v in cols_rename.items() if k in gdf_rod.columns})
    
    # Aplica a faxina de texto nas colunas categóricas
    cols_limpar = ['codigo_rodovia', 'jurisdicao', 'revestimento', 'tipo_pavimento', 'situacao_fisica', 'status_operacional']
    for col in cols_limpar:
        if col in gdf_rod.columns:
            gdf_rod[col] = gdf_rod[col].apply(remover_acentos)

    # Converte a geometria para texto (WKT) e remove a coluna de geometria complexa
    gdf_rod['geometria_wkt'] = gdf_rod.geometry.to_wkt()
    df_rod = pd.DataFrame(gdf_rod.drop(columns=['geometry']))
    
    # Cria um ID único para a linha
    df_rod['id_trecho_rodoviario'] = df_rod.index + 1
    
    # Joga o ID pra frente
    cols = df_rod.columns.tolist()
    cols = ['id_trecho_rodoviario'] + [c for c in cols if c != 'id_trecho_rodoviario']
    df_rod = df_rod[cols]

    df_rod.to_csv(out_rodovias, index=False, encoding='utf-8')
    print(f"✅ Rodovias salvas: {len(df_rod)} trechos convertidos para WKT.")
else:
    print(f"⚠️ Aviso: {file_rodovias} não encontrado. Pulando...")


# ==========================================
# 3. Processando Linhas de Transmissão (ONS) - COM REGEX
# ==========================================
file_ons_linhas = os.path.join(GEO_DIR, 'linhas_transmissao_ons.geojson')
out_ons_linhas = os.path.join(DIM_DIR, 'dim_ons_linhas_transmissao.csv')

if os.path.exists(file_ons_linhas):
    print("\nLendo Linhas de Transmissão (ONS)...")
    gdf_lt = gpd.read_file(file_ons_linhas)
    
    # Extração via Expressão Regular do HTML sujo da coluna PopupInfo
    def extrair_tensao(html):
        match = re.search(r'<b>Tensão:\s*<\/b>(.*?)(?:<br\/>|$)', str(html), re.IGNORECASE)
        return match.group(1).replace(' Kv', '').strip() if match else '0'

    def extrair_extensao(html):
        match = re.search(r'<b>Extensão:\s*<\/b>(.*?)(?:<br\/>|$)', str(html), re.IGNORECASE)
        return match.group(1).replace(' Km', '').replace(',', '.').strip() if match else '0'

    gdf_lt['nome_linha'] = gdf_lt['Name'].apply(remover_acentos)
    gdf_lt['tensao_kv'] = gdf_lt['PopupInfo'].apply(extrair_tensao)
    gdf_lt['extensao_km'] = gdf_lt['PopupInfo'].apply(extrair_extensao)
    
    # Converte tipos numéricos para o Druid poder somar/calcular média depois
    gdf_lt['tensao_kv'] = pd.to_numeric(gdf_lt['tensao_kv'], errors='coerce').fillna(0)
    gdf_lt['extensao_km'] = pd.to_numeric(gdf_lt['extensao_km'], errors='coerce').fillna(0)
        
    gdf_lt['geometria_wkt'] = gdf_lt.geometry.to_wkt()
    df_lt = pd.DataFrame(gdf_lt)
    df_lt['id_linha_transmissao'] = df_lt.index + 1
    
    # Seleciona apenas as colunas limpas e úteis
    colunas_limpas_lt = ['id_linha_transmissao', 'nome_linha', 'tensao_kv', 'extensao_km', 'Shape_Length', 'geometria_wkt']
    df_lt = df_lt[[c for c in colunas_limpas_lt if c in df_lt.columns]]
    
    df_lt.to_csv(out_ons_linhas, index=False, encoding='utf-8')
    print(f"✅ Linhas ONS salvas. Tags HTML removidas e métricas extraídas.")

# ==========================================
# 4. Processando Subestações (ONS) - COM REGEX
# ==========================================
file_ons_sub = os.path.join(GEO_DIR, 'subestacoes_ons.geojson')
out_ons_sub = os.path.join(DIM_DIR, 'dim_ons_subestacoes.csv')

if os.path.exists(file_ons_sub):
    print("\nLendo Subestações (ONS)...")
    gdf_sub = gpd.read_file(file_ons_sub)
    
    # Extração via Expressão Regular
    def extrair_capacidade(html):
        match = re.search(r'<b>Capacidade:\s*<\/b>(.*?)(?:<br\/>|$)', str(html), re.IGNORECASE)
        return match.group(1).replace(' MW', '').replace(',', '.').strip() if match else '0'

    def extrair_agente(html):
        match = re.search(r'<b>Agente:\s*<\/b>(.*?)(?:<br\/>|$)', str(html), re.IGNORECASE)
        return remover_acentos(match.group(1).strip()) if match else 'NAO INFORMADO'

    gdf_sub['nome_subestacao'] = gdf_sub['Name'].apply(remover_acentos)
    gdf_sub['capacidade_mw'] = gdf_sub['PopupInfo'].apply(extrair_capacidade)
    gdf_sub['agente_responsavel'] = gdf_sub['PopupInfo'].apply(extrair_agente)
    
    gdf_sub['capacidade_mw'] = pd.to_numeric(gdf_sub['capacidade_mw'], errors='coerce').fillna(0)
    
    # Extrai a coordenada exata
    gdf_sub['latitude'] = gdf_sub.geometry.y
    gdf_sub['longitude'] = gdf_sub.geometry.x
    
    df_sub = pd.DataFrame(gdf_sub)
    df_sub['id_subestacao'] = df_sub.index + 1
    
    # Filtra o lixo do Google Earth
    colunas_limpas_sub = ['id_subestacao', 'nome_subestacao', 'capacidade_mw', 'agente_responsavel', 'latitude', 'longitude']
    df_sub = df_sub[[c for c in colunas_limpas_sub if c in df_sub.columns]]
    
    df_sub.to_csv(out_ons_sub, index=False, encoding='utf-8')
    print(f"✅ Subestações ONS salvas. Capacidade e Agente isolados perfeitamente.")



print("\n==================================================")
print("🎉 FASE DE ETL (EXTRAÇÃO E TRANSFORMAÇÃO) CONCLUÍDA! 🎉")
print("==================================================")