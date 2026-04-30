# src/streaming/processador.py

import pandas as pd
import math
import unicodedata

class ProcessadorFocos:
    def __init__(self, motor_espacial):
        self.motor = motor_espacial

    @staticmethod
    def limpar_texto(texto):
        if pd.isna(texto) or not str(texto).strip(): 
            return "NAO INFORMADO"
        texto = str(texto).replace('\n', ' ').replace('\r', ' ').strip().upper()
        return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

    @staticmethod
    def limpar_valor_numerico(valor):
        """
        Tratamento de Outliers e Erros:
        1. Converte NaN/Inf para 0.0.
        2. Converte o outlier do INPE (-999.0) para 0.0.
        """
        try:
            f_valor = float(valor)
            # Se for NaN, Infinito ou o código de erro do INPE (-999)
            if math.isnan(f_valor) or math.isinf(f_valor) or f_valor <= -900:
                return 0.0
            return f_valor
        except:
            return 0.0

    def transformar(self, linha_bruta):
        """Transforma e enriquece o dado bruto para o schema final."""
        
        dt_hora = pd.to_datetime(linha_bruta.get('DataHora'), errors='coerce')
        estado_norm = self.limpar_texto(linha_bruta.get('Estado'))
        municipio_norm = self.limpar_texto(linha_bruta.get('Municipio'))
        
        lat = float(linha_bruta.get('Latitude', 0.0))
        lon = float(linha_bruta.get('Longitude', 0.0))

        # Aciona o motor espacial (Certifique-se que o motor_espacial.py 
        # retorna todas essas chaves no dicionário 'analise')
        analise = self.motor.analisar_todas_ameacas(lat, lon)

        return {
            "id_data": int(dt_hora.strftime('%Y%m%d')) if pd.notna(dt_hora) else 0,
            "data_hora": dt_hora.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(dt_hora) else "1970-01-01 00:00:00",
            "chave_localidade": f"{estado_norm}_{municipio_norm}",
            
            # --- Chaves de Infraestrutura (CRUCIAL PARA O DRUID) ---
            "codigo_ti": analise.get('codigo_ti', -1),
            "codigo_ibge_espacial": analise.get('codigo_ibge_espacial', -1),
            "id_trecho_rodoviario": analise.get('id_trecho_rodoviario', -1),
            "distancia_rodovia_km": analise.get('distancia_rodovia_km', -1.0),
            "id_linha_transmissao": analise.get('id_linha_transmissao', -1),
            "distancia_linha_km": analise.get('distancia_linha_km', -1.0),
            "id_subestacao": analise.get('id_subestacao', -1),
            "distancia_subestacao_km": analise.get('distancia_subestacao_km', -1.0),
            "id_sistema_isolado": analise.get('id_sistema_isolado', -1),
            "distancia_sistema_isolado_km": analise.get('distancia_sistema_isolado_km', -1.0),
            "codigo_cnes_prox": analise.get('codigo_cnes_prox', -1),
            "distancia_hospital_km": analise.get('distancia_hospital_km', -1.0),
            
            "latitude": lat,
            "longitude": lon,
            "bioma_inpe": self.limpar_texto(linha_bruta.get('Bioma')),
            "satelite_sensor": self.limpar_texto(linha_bruta.get('Satelite')),
            
            # --- Métricas com limpeza de Outliers ---
            "dias_sem_chuva": self.limpar_valor_numerico(linha_bruta.get('DiaSemChuva')),
            "precipitacao_mm": self.limpar_valor_numerico(linha_bruta.get('Precipitacao')),
            "risco_fogo_inpe": self.limpar_valor_numerico(linha_bruta.get('RiscoFogo')),
            "potencia_radiativa_fogo": self.limpar_valor_numerico(linha_bruta.get('FRP'))
        }