import requests
import pandas as pd
from datetime import datetime
import time
from flask import Flask, jsonify
import psycopg2
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor
import gc

# Definindo constantes globais
DATA_INICIAL = (datetime.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
TODAY = datetime.today().strftime('%Y-%m-%d')
BASE_URL = "http://hidro.tach.com.br/exportar.php?id={}&data1={}&data2={}"
USERNAME = "brk"
PASSWORD = "saneatins"
TEMPO_ESPERA = 5
CABECALHOS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
}

# Inicializando o aplicativo Flask
app = Flask(__name__)

def salvar_dados_com_insert(dados_para_inserir, tabela="dados_barragens"):
    conn_str = 'postgresql://postgres:7sw0F2MNx0ObN32g@singly-light-topi.data-1.use1.tembo.io:5432/postgres'
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                for dados in dados_para_inserir:
                    barragem, data_e_hora, nivel_m, volume_mm = dados

                    # Verificar se os dados já existem
                    query_check = """
                    SELECT 1 FROM {tabela} WHERE "Data e Hora" = %s AND barragem = %s
                    """
                    cursor.execute(sql.SQL(query_check).format(tabela=sql.Identifier(tabela)), (data_e_hora, barragem))

                    if cursor.fetchone() is None:
                        query_insert = """
                        INSERT INTO {tabela} (barragem, "Data e Hora", "Nível (m)", "Volume (mm)")
                        VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(sql.SQL(query_insert).format(tabela=sql.Identifier(tabela)), dados)

                conn.commit()

    except Exception as e:
        print(f"Erro ao inserir os dados: {e}")


def obter_dados(barragem):
    barragemID, barragemNome = barragem
    url_nivel = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=nivel"
    url_chuva = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=chuva"
    auth = (USERNAME, PASSWORD)

    try:
        response_nivel = requests.get(url_nivel, auth=auth, headers=CABECALHOS, timeout=TEMPO_ESPERA)
        response_nivel.raise_for_status()
        table_nivel = pd.read_html(response_nivel.text)[0]
    except Exception as e:
        print(f"Erro ao obter dados de nível da barragem {barragemNome}: {e}")
        return None

    try:
        response_chuva = requests.get(url_chuva, auth=auth, headers=CABECALHOS, timeout=TEMPO_ESPERA)
        response_chuva.raise_for_status()
        table_chuva = pd.read_html(response_chuva.text)[0]
    except Exception as e:
        print(f"Erro ao obter dados de chuva da barragem {barragemNome}: {e}")
        return None

    table_nivel.columns = ["Código Estação", "Data e Hora", "Nível (m)"]
    table_chuva.columns = ["Código Estação", "Data e Hora", "Volume (mm)"]
    table_nivel.dropna(inplace=True)
    table_chuva.dropna(inplace=True)

    merged = pd.merge(table_nivel, table_chuva, on="Data e Hora", how="left")
    merged["Data e Hora"] = pd.to_datetime(merged["Data e Hora"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    merged.dropna(subset=["Data e Hora"], inplace=True)

    merged["BARRAGEM"] = barragemNome
    merged = merged[["BARRAGEM", "Data e Hora", "Nível (m)", "Volume (mm)"]]
    merged["Nível (m)"] = pd.to_numeric(merged["Nível (m)"], downcast="float")
    merged["Volume (mm)"] = pd.to_numeric(merged["Volume (mm)"], downcast="float")

    return [tuple(x) for x in merged.to_numpy()]

@app.route("/coletar", methods=["GET"])
def get_dados():
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

    def processar_barragem(barragem):
        dados = obter_dados(barragem)
        if dados:
            salvar_dados_com_insert(dados)
        gc.collect()  # Forçar limpeza de memória

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processar_barragem, lista_barragens)

    return jsonify({"message": "Dados processados e inseridos com sucesso!"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

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

    