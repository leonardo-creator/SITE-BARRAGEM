'''# Importando bibliotecas necessárias
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from termcolor import colored
import time
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

# Definindo constantes globais
DATA_INICIAL = "2024-07-01"
TODAY = datetime.today().strftime('%Y-%m-%d')
BASE_URL = "http://hidro.tach.com.br/exportar.php?id={}&data1={}&data2={}"
USERNAME = "brk"
PASSWORD = "saneatins"
TEMPO_ESPERA = 60
CABECALHOS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
}

def obter_dados(barragem):
    """Função para coletar dados de uma barragem específica."""
    barragemID, barragemNome = barragem

    # Construindo URLs
    url_nivel = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=nivel"
    url_chuva = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=chuva"
    auth = (USERNAME, PASSWORD)

    # Realizando a requisição para obter dados de nível
    try:
        print(colored(f"Buscando dados de nível da barragem {barragemNome} ...", "yellow"))
        response_nivel = requests.get(url_nivel, auth=auth, headers=CABECALHOS, timeout=TEMPO_ESPERA)
        response_nivel.raise_for_status()
        table_nivel = pd.read_html(response_nivel.text)[0]
        print(colored(f"Dados brutos da barragem {barragemNome} (Nível):\n", "green"), table_nivel.head())
    except Exception as e:
        print(colored(f"Erro ao obter dados de nível da barragem {barragemNome}: {e}", "red"))
        return None

    # Realizando a requisição para obter dados de chuva
    try:
        print(colored(f"Buscando dados de chuva da barragem {barragemNome} ...", "yellow"))
        response_chuva = requests.get(url_chuva, auth=auth, headers=CABECALHOS, timeout=TEMPO_ESPERA)
        response_chuva.raise_for_status()
        table_chuva = pd.read_html(response_chuva.text)[0]
        print(colored(f"Dados brutos da barragem {barragemNome} (Chuva):\n", "green"), table_chuva.head())
    except Exception as e:
        print(colored(f"Erro ao obter dados de chuva da barragem {barragemNome}: {e}", "red"))
        return None

    # Processando e limpando os dados
    table_nivel.columns = ["Código Estação", "Data e Hora", "Nível (m)"]
    table_chuva.columns = ["Código Estação", "Data e Hora", "Volume (mm)"]
    table_nivel.dropna(inplace=True)
    table_chuva.dropna(inplace=True)
    merged = pd.merge(table_nivel, table_chuva, on="Data e Hora", how="left")

    # Convertendo a coluna "Data e Hora" para o tipo datetime
    def try_convert(date_str):
        try:
            return pd.to_datetime(date_str, format="%d/%m/%Y %H:%M:%S", dayfirst=True)
        except:
            return pd.NaT

    merged["Data e Hora"] = merged["Data e Hora"].apply(try_convert)
    merged.dropna(subset=["Data e Hora"], inplace=True)

    # Completando o dataframe com informações adicionais
    merged["BARRAGEM"] = barragemNome
    merged = merged[["BARRAGEM", "Data e Hora", "Nível (m)", "Volume (mm)"]]
    merged["Nível (m)"] = merged["Nível (m)"].astype(float)
    merged["Volume (mm)"] = merged["Volume (mm)"].astype(float)

    print(colored(f"Dados da barragem {barragemNome} coletados e processados com sucesso!", "green"))

    return merged


# Inicializando a coleta de dados
print(colored("\nIniciando coleta de dados...", "cyan"))

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
    ("234", "ETE Aureny")]

lista_barragens = [
    ("175", "Barragem São João"),]

# Coleta de dados paralela das barragens
with ThreadPoolExecutor(max_workers=1) as executor:
    dfs = list(executor.map(obter_dados, lista_barragens))

# Consolidação dos dados coletados em um único DataFrame
resultado_final = pd.concat(dfs, ignore_index=True)

# Apresentação dos primeiros dados consolidados
print(colored("\nDados combinados de todas as barragens:\n", "blue"), resultado_final.head())

resultado_final = resultado_final.dropna(subset=["Data e Hora"])
resultado_final["Nível (m)"] = resultado_final["Nível (m)"] / 100

# Salvando os dados processados em um arquivo CSV
print(colored("\nSalvando dados em 'retilineo2.csv'...", "cyan"))
resultado_final.to_csv('retilineo.csv', index=False)
print(colored("Dados salvos em 'retilineo.csv'!", "green")) '''




# Função para salvar os dados no banco de dados
# Configurações do banco de dados
import psycopg2

def conect():
    # Connection strisableing
    conn_str = 'postgresql://postgres:7sw0F2MNx0ObN32g@singly-light-topi.data-1.use1.tembo.io:5432/postgres'

    try:
        # Create a new database session
        conn = psycopg2.connect(conn_str)
        print("Connected to the database successfully.")

        # Create a new cursor object.
        cur = conn.cursor()

        # Test Query
        cur.execute("SELECT 1")

        # Fetch result
        output = cur.fetchone()[0]
        print(output)
        
    except Exception as e:
        print(f"Unable to connect to the database or execute query: {e}")
    
    finally:
        # Ensure cursor and connection are closed only if they were opened
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Connection and cursor closed.")

# Call the function
conect()




