import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from termcolor import colored

# Constantes
DATA_INICIAL = "2000-07-01"
TODAY = datetime.today().strftime('%Y-%m-%d')
BASE_URL = "http://hidro.tach.com.br/exportar.php?id={}&data1={}&data2={}"
USERNAME = "brk"
PASSWORD = "saneatins"
TIMEOUT = 60
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.3'
}

def obter_dados(barragem):
    barragemID, barragemNome = barragem
    print(colored(f"\nBuscando dados da barragem: {barragemNome} ...", "yellow"))

    url_nivel = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=nivel"
    url_chuva = BASE_URL.format(barragemID, DATA_INICIAL, TODAY) + "&tipo=chuva"
    
    auth = (USERNAME, PASSWORD)

    try:
        response_nivel = requests.get(url_nivel, auth=auth, headers=HEADERS, timeout=TIMEOUT)
        response_nivel.raise_for_status()
        table_nivel = pd.read_html(response_nivel.text)[0]
        print(colored(f"Dados brutos da barragem {barragemNome} (Nível):\n", "green"), table_nivel.head())
    except Exception as e:
        print(colored(f"Erro ao obter dados de nível da barragem {barragemNome}: {e}", "red"))
        print("Resposta do servidor:", response_nivel.text)
        return None

    try:
        response_chuva = requests.get(url_chuva, auth=auth, headers=HEADERS, timeout=TIMEOUT)
        response_chuva.raise_for_status()
        table_chuva = pd.read_html(response_chuva.text)[0]
        print(colored(f"Dados brutos da barragem {barragemNome} (Chuva):\n", "green"), table_chuva.head())
    except Exception as e:
        print(colored(f"Erro ao obter dados de chuva da barragem {barragemNome}: {e}", "red"))
        print("Resposta do servidor:", response_chuva.text)
        return None

    table_nivel.columns = ["Código Estação", "Data e Hora", "Nível (m)"]
    table_chuva.columns = ["Código Estação", "Data e Hora", "Volume (mm)"]

    table_nivel.dropna(inplace=True)
    table_chuva.dropna(inplace=True)

    merged = pd.merge(table_nivel, table_chuva, on="Data e Hora", how="left")

    def try_convert(date_str):
        try:
            return pd.to_datetime(date_str, format="%d/%m/%Y %H:%M:%S", dayfirst=True)
        except:
            return pd.NaT

    merged["Data e Hora"] = merged["Data e Hora"].apply(try_convert)
    merged.dropna(subset=["Data e Hora"], inplace=True)

    merged["BARRAGEM"] = barragemNome
    merged = merged[["BARRAGEM", "Data e Hora", "Nível (m)", "Volume (mm)"]]
    merged["Nível (m)"] = merged["Nível (m)"].astype(float)
    merged["Volume (mm)"] = merged["Volume (mm)"].astype(float)

    return merged



# Lista de barragens
lista_barragens = [
    ("175", "Barragem São João"),
    ("176", "Barragem do Papagaio"),
    ("177", "Barragem Santo Antonio"),
    ("178", "Barragem Buritis"),
    ("179", "Barragem Cocalinho"),
    ("180", "Barragem Piaus"),
    ("181", "Barragem Bananal"),
    ("182", "Barragem Do Coco"),
    ("183", "Barragem Agua Franca"),
    ("184", "Barragem Agua Fria"),
    ("185", "Barragem Campeira"),
    ("188", "Barragem Horto I"),
    ("189", "Barragem Caravlhal"),
    ("190", "Barragem Ribeirao Pinhal"),
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


# Buscar dados de cada barragem
print("\nIniciando coleta de dados...")
dfs = [obter_dados(id, nome) for id, nome in lista_barragens]

# Combinar os resultados
resultado_final = pd.concat(dfs, ignore_index=True)
print("\nDados combinados de todas as barragens:\n", resultado_final.head())

# Realizar transformações adicionais
print("\nRealizando transformações adicionais...")
resultado_final["Data 2020"] = resultado_final["Data e Hora"].apply(lambda x: datetime(2020, x.month, x.day))
resultado_final["Ano2"] = resultado_final["Data e Hora"].dt.year
resultado_final["ANO"] = resultado_final["Data e Hora"].dt.year
resultado_final["Ano"] = resultado_final["Data e Hora"].dt.year
resultado_final = resultado_final.dropna(subset=["Data e Hora"])

# Dividir a coluna "Nível (m)"
resultado_final["Nível (m)"] = resultado_final["Nível (m)"] / 100

# Salvar em CSV
print("\nSalvando dados em 'resultado.csv'...")
resultado_final.to_csv('retilineo.csv', index=False)
print("Processo concluído!")

# Criar pivôs
print("\nCriando tabelas pivô...")
nivel_pivot = pd.pivot_table(resultado_final, values='Nível (m)', index=["BARRAGEM", "Data e Hora", "Data 2020"], columns=['ANO'], aggfunc='sum').reset_index()
nivel_pivot.columns = [str(col) + " NIVEL" if isinstance(col, int) else col for col in nivel_pivot.columns]

chuva_pivot = pd.pivot_table(resultado_final, values='Volume (mm)', index=["BARRAGEM", "Data e Hora", "Data 2020"], columns=['Ano2'], aggfunc='sum').reset_index()
chuva_pivot.columns = [str(col) + " CHUVA" if isinstance(col, int) else col for col in chuva_pivot.columns]

# Mesclar pivôs
print("\nMesclando tabelas pivô...")
merged_df = pd.merge(nivel_pivot, chuva_pivot, on=["BARRAGEM", "Data e Hora", "Data 2020"], how='left')
merged_df = merged_df.rename(columns={"Data 2020": "MÊS DIA"})

print("\nResultado final:\n", merged_df.head())

# Salvar em CSV
print("\nSalvando dados em 'resultado.csv'...")
merged_df.to_csv('resultado.csv', index=False)
print("Processo concluído!")