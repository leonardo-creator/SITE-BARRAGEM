from flask import Flask, jsonify
import requests
import pandas as pd
from datetime import datetime
import time
import psycopg2
from psycopg2 import sql
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Definindo constantes globais
DATA_INICIAL = (datetime.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
TODAY = datetime.today().strftime('%Y-%m-%d')
BASE_URL = "http://hidro.tach.com.br/exportar.php?id={}&data1={}&data2={}"
USERNAME = "brk"
PASSWORD = "saneatins"
TEMPO_ESPERA = 5
CABECALHOS = {'User-Agent': 'Mozilla/5.0'}
CONN_STR = 'postgresql://postgres:7sw0F2MNx0ObN32g@singly-light-topi.data-1.use1.tembo.io:5432/postgres'

# Lista de barragens
lista_barragens = [
    ("175", "Barragem São João"),
    ("176", "Barragem do Papagaio"),
    ("177", "Barragem Santo Antônio"),
    ("178", "Barragem Buritis"),
    ("179", "Barragem Cocalhinho"),
    ("180", "Barragem Piaus"),
    ("181", "Barragem Bananal"),
    ("182", "Barragem Do Coco"),
    ("183", "Barragem Água Franca"),
    ("184", "Barragem Água Fria"),
    ("185", "Barragem Campeira"),
    ("188", "Barragem Horto I"),
    ("189", "Barragem Carvalhal"),
    ("190", "Barragem Ribeirão Pinhal"),
    ("191", "Barragem Natividade"),
    ("193", "Barragem Urubuzinho"),
    ("194", "Barragem Fiscal"),
    ("195", "Palmas ETA 003"),
    ("197", "Barragem Garrafinha"),
    ("198", "Barragem Rio Jaguari"),
    ("200", "Palmas ETA 006"),
    ("201", "Barragem Pernada"),
    ("202", "Barragem Zuador"),
    ("203", "Barragem Xinguara"),
    ("204", "Captacao São Borges"),
    ("209", "Barragem Horto II"),
    ("208", "Barragem Marcelo"),
    ("207", "Palmas ETA 007"),
    ("238", "UTS 002"),
    ("237", "Operacional 03"),
    ("236", "Centro De Reservação"),
    ("235", "ETE Santa Fe"),
    ("234", "ETE Aureny")
]

def obter_dados(barragem):
    """Função para coletar dados de uma barragem específica."""
    barragemID, barragemNome = barragem
    url_nivel = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=nivel"
    url_chuva = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=chuva"
    auth = (USERNAME, PASSWORD)

    try:
        logging.info(f"Coletando dados de nível para {barragemNome}")
        response_nivel = requests.get(url_nivel, auth=auth, headers=CABECALHOS, timeout=TEMPO_ESPERA)
        response_nivel.raise_for_status()
        table_nivel = pd.read_html(response_nivel.text)[0]
    except Exception as e:
        return f"Erro ao obter dados de nível da barragem {barragemNome}: {e}", None

    try:
        logging.info(f"Coletando dados de chuva para {barragemNome}")
        response_chuva = requests.get(url_chuva, auth=auth, headers=CABECALHOS, timeout=TEMPO_ESPERA)
        response_chuva.raise_for_status()
        table_chuva = pd.read_html(response_chuva.text)[0]
    except Exception as e:
        return f"Erro ao obter dados de chuva da barragem {barragemNome}: {e}", None

    # Processamento dos dados
    try:
        logging.info(f"Processando dados para {barragemNome}")
        table_nivel.columns = ["Código Estação", "Data e Hora", "Nível (m)"]
        table_chuva.columns = ["Código Estação", "Data e Hora", "Volume (mm)"]
        merged = pd.merge(table_nivel, table_chuva, on="Data e Hora", how="left")
        merged["Data e Hora"] = pd.to_datetime(merged["Data e Hora"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
        merged["BARRAGEM"] = barragemNome
        merged = merged[["BARRAGEM", "Data e Hora", "Nível (m)", "Volume (mm)"]]
        return None, merged
    except Exception as e:
        return f"Erro ao processar dados da barragem {barragemNome}: {e}", None

def salvar_dados_com_insert(df, tabela="dados_barragens"):
    """Função para salvar dados no banco de dados."""
    try:
        conn = psycopg2.connect(CONN_STR)
        cursor = conn.cursor()
        for _, row in df.iterrows():
            dados = (row["BARRAGEM"], row["Data e Hora"], row["Nível (m)"], row["Volume (mm)"])
            query_check = f"""SELECT 1 FROM {tabela} WHERE "Data e Hora" = %s AND barragem = %s"""
            cursor.execute(query_check, (row["Data e Hora"], row["BARRAGEM"]))
            if cursor.fetchone() is None:
                query_insert = f"""
                INSERT INTO {tabela} (barragem, "Data e Hora", "Nível (m)", "Volume (mm)")
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(query_insert, dados)
        conn.commit()
    except Exception as e:
        logging.error(f"Erro ao salvar dados no banco: {e}")
        return f"Erro ao salvar dados no banco: {e}"
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
    return None

@app.route('/coletar', methods=['GET'])
def coletar():
    """Rota para iniciar a coleta de dados."""
    resultados = []
    for barragem in lista_barragens:
        erro, df = obter_dados(barragem)
        if erro:
            logging.warning(erro)
            resultados.append(erro)
        elif df is not None and not df.empty:
            erro_bd = salvar_dados_com_insert(df)
            if erro_bd:
                resultados.append(erro_bd)
            else:
                resultados.append(f"Dados da barragem {barragem[1]} salvos com sucesso.")
        else:
            resultados.append(f"Sem dados disponíveis para a barragem {barragem[1]}.")
        time.sleep(TEMPO_ESPERA)

    return jsonify(resultados), 200

if __name__ == '__main__':
    app.run(debug=True)