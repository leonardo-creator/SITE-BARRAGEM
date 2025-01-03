import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
from flask import Flask, jsonify
import psycopg2
from psycopg2 import sql

# Definindo constantes globais
DATA_INICIAL = (datetime.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
TODAY = datetime.today().strftime('%Y-%m-%d')
BASE_URL = "http://hidro.tach.com.br/exportar.php?id={}&data1={}&data2={}"
USERNAME = "brk"
PASSWORD = "saneatins"
CABECALHOS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
}
CONN_STR = 'postgresql://postgres:7sw0F2MNx0ObN32g@singly-light-topi.data-1.use1.tembo.io:5432/postgres'

# Inicializando o aplicativo Flask
app = Flask(__name__)

async def fetch_data(session, url):
    async with session.get(url, auth=aiohttp.BasicAuth(USERNAME, PASSWORD), headers=CABECALHOS) as response:
        if response.status == 200:
            return await response.text()
        return None

async def process_barragem(barragem):
    barragemID, barragemNome = barragem
    url_nivel = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=nivel"
    url_chuva = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=chuva"

    async with aiohttp.ClientSession() as session:
        try:
            html_nivel = await fetch_data(session, url_nivel)
            html_chuva = await fetch_data(session, url_chuva)

            if not html_nivel or not html_chuva:
                print(f"Erro ao obter dados da barragem {barragemNome}.")
                return []

            table_nivel = pd.read_html(html_nivel)[0]
            table_chuva = pd.read_html(html_chuva)[0]

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

            return [tuple(row) for row in merged.to_numpy()]
        except Exception as e:
            print(f"Erro no processamento da barragem {barragemNome}: {e}")
            return []

def save_to_db(data):
    try:
        with psycopg2.connect(CONN_STR) as conn:
            with conn.cursor() as cursor:
                insert_query = """
                INSERT INTO dados_barragens (barragem, "Data e Hora", "Nível (m)", "Volume (mm)")
                VALUES %s ON CONFLICT DO NOTHING
                """
                psycopg2.extras.execute_values(cursor, insert_query, data, page_size=1000)
            conn.commit()
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")

@app.route("/coletar", methods=["GET"])
async def get_dados():
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


    tasks = [process_barragem(barragem) for barragem in lista_barragens]
    results = await asyncio.gather(*tasks)

    # Unir todos os dados processados
    all_data = [item for sublist in results for item in sublist]

    # Salvar no banco
    save_to_db(all_data)

    return jsonify({"message": "Dados processados e inseridos com sucesso!"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

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
