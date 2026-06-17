import requests
import os
import zipfile
import geopandas as gpd
import pandas as pd
import pendulum
import yaml
import re
from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

# Airflow
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

SUBPASTA_ASSUNTO = os.path.dirname(os.path.abspath(__file__))
RAIZ_DAGS_DIR = os.path.dirname(SUBPASTA_ASSUNTO)
CONFIG_PATH = os.path.join(RAIZ_DAGS_DIR, "config.yaml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

###########################################################VARIAVEIS###########################################################
LESSONIA_USER = config["databases"]["lessonia"]["user"]
LESSONIA_PASS = config["databases"]["lessonia"]["password"]
LESSONIA_HOST = config["databases"]["lessonia"]["host"]
LESSONIA_PORT = config["databases"]["lessonia"]["port"]
LESSONIA_NAME = config["databases"]["lessonia"]["name"]

LESSONIA_DATA_LOCAL = config["dir"]["lessonia"]["local_data"]

now = datetime.now()
year = now.year
month = now.month
day = now.day

# URL Raiz para Malhas Estaduais (UFs)
URL_RAIZ_IBGE = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/"
caminho = f"{LESSONIA_DATA_LOCAL}/BR_UF_{year}_{month}_{day}.zip"
DATABASE_URL = f'postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}'
################################################################################################################################

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
}

def obter_url_e_data_mais_recente_uf(url_raiz):
    """
    Raspa a página do IBGE buscando o arquivo BR_UF_XXXX.zip mais recente
    e extrai a data real de modificação via Regex.
    """
    try:
        if not url_raiz.endswith('/'):
            url_raiz += '/'

        # 1. Identifica o ano mais recente na raiz (municipio_2025/)
        res_raiz = requests.get(url_raiz, headers=HEADERS, timeout=60)
        if res_raiz.status_code != 200:
            return None, None

        soup_raiz = BeautifulSoup(res_raiz.text, 'html.parser')
        anos = []
        for link in soup_raiz.find_all('a'):
            href = link.get('href', '').strip()
            if 'municipio_' in href:
                ano_str = "".join(filter(str.isdigit, href))
                if ano_str:
                    anos.append(int(ano_str))

        ano_mais_recente = sorted(anos, reverse=True)[0] if anos else 2025

        # 2. Define o caminho final de raspagem das UFs
        url_pasta_final = f"{url_raiz}municipio_{ano_mais_recente}/Brasil/"
        res_final = requests.get(url_pasta_final, headers=HEADERS, timeout=60)
        if res_final.status_code != 200:
            return None, None

        soup_final = BeautifulSoup(res_final.text, 'html.parser')

        # O alvo agora passa a ser br_uf_2025.zip
        nome_arquivo_alvo = f"br_uf_{ano_mais_recente}.zip"

        url_zip_final = None
        data_real = None

        # 3. Varre os blocos HTML buscando o arquivo e a data real por Regex
        for elemento in soup_final.find_all(['tr', 'div', 'li']):
            texto_bloco = elemento.text.strip().lower()

            if nome_arquivo_alvo in texto_bloco:
                link_attr = elemento.find('a')
                if link_attr:
                    href_real = link_attr.get('href', '').strip()
                    url_zip_final = f"{url_pasta_final}{href_real}"

                padrao_data = re.search(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}', elemento.text)
                if padrao_data:
                    data_str = padrao_data.group(0)
                        try:
                            data_real = datetime.strptime(data_str, '%Y-%m-%d %H:%M')
                            break
                        except ValueError:
                            continue

        if url_zip_final and data_real:
            print(f"[IBGE UF CAPTURA] Arquivo: {nome_arquivo_alvo}")
            print(f"[IBGE UF CAPTURA] Data real encontrada no site: {data_real}")
            return url_zip_final, data_real

        print("[IBGE UF CAPTURA] Não foi possível extrair a combinação de arquivo e data real de UF.")
        return None, None

    except Exception as e:
        print(f"Erro ao raspar estrutura de UF do IBGE: {e}")
        return None, None


@task.branch(task_id="check_dates")
def check_dates():
    url_dinamica, last_modified = obter_url_e_data_mais_recente_uf(URL_RAIZ_IBGE)

    if not url_dinamica or not last_modified:
        print("Não foi possível determinar o arquivo de UF mais recente no IBGE.")
        return 'end'

    print(f"=== DEBUG LOGS UF ===")
    print(f"VALOR PEGO NO SITE (IBGE): {last_modified} | Tipo: {type(last_modified)}")

    engine = create_engine(DATABASE_URL)
    # Alvo alterado para a tabela public.ibge_ufs
    sql = "select distinct (last_modified) from public.ibge_ufs"

    try:
        df_data = pd.read_sql(sql, con=engine)
        if not df_data.empty:
            last_date = pd.to_datetime(df_data['last_modified'].iloc[0]).replace(tzinfo=None)
            print(f"VALOR PEGO NO BANCO (POSTGRES): {last_date} | Tipo: {type(last_date)}")
            print(f"==================")

            if last_modified.replace(tzinfo=None) <= last_date:
                print('Não existe atualizações de dados novos de UF no IBGE.')
                return 'end'
        else:
            print("A tabela ibge_ufs está vazia. Seguindo para o download.")
    except Exception as e:
        print(f"Erro ao consultar banco (tabela pode não existir): {e}")

    return 'download_data'


@task(task_id="download_data")
def download_data(destino_local):
    url_dinamica, _ = obter_url_e_data_mais_recente_uf(URL_RAIZ_IBGE)

    if not url_dinamica:
        raise ValueError("URL dinâmica de download de UF não foi encontrada.")

    print(f"Iniciando download do arquivo dinâmico de UF: {url_dinamica}")
    response = requests.get(url_dinamica, headers=HEADERS, stream=True, timeout=60)

    if response.status_code != 200:
        raise ConnectionError(f"Não foi possível baixar o arquivo de UF. Status: {response.status_code}")

    os.makedirs(os.path.dirname(destino_local), exist_ok=True)

    with open(destino_local, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)

    return destino_local


@task(task_id="truncate_table")
def truncate_table(caminho_arquivo):
    engine = create_engine(DATABASE_URL)
    print("Iniciando TRUNCATE na tabela public.ibge_ufs...")
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.ibge_ufs;"))
        print("Tabela public.ibge_ufs truncada com sucesso!")
    except Exception as e:
        print(f"Erro ao aplicar TRUNCATE na ibge_ufs: {e}")

    return caminho_arquivo


@task(task_id="upload_data")
def upload_data(caminho):
    _, last_modified = obter_url_e_data_mais_recente_uf(URL_RAIZ_IBGE)

    pasta_extracao = os.path.join(os.path.dirname(caminho), "extracao_ibge_uf")
    os.makedirs(pasta_extracao, exist_ok=True)

    with zipfile.ZipFile(caminho, 'r') as zip_ref:
        zip_ref.extractall(pasta_extracao)

    arquivos_shp = [os.path.join(root, name)
                    for root, dirs, files in os.walk(pasta_extracao)
                    for name in files if name.endswith('.shp')]

    if not arquivos_shp:
        raise FileNotFoundError("Nenhum arquivo .shp encontrado no ZIP extraído.")

    df = gpd.read_file(arquivos_shp[0])
    df['last_modified'] = last_modified

    engine = create_engine(DATABASE_URL)
    # Alvo alterado para despejar na tabela ibge_ufs
    df.to_postgis(name='ibge_ufs', con=engine, if_exists='replace', index=False)
    print("Dados de UF inseridos com sucesso!")


@dag(
    dag_id="IBGE_UF",
    schedule='0 11 * * *',  # Roda às 11:00 UTC (uma hora após a de municípios)
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 25),
    tags=['daily', 'transformation'],
    max_active_runs=1
)
def ibge_uf_update_task_flow():
    branches = check_dates()

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    dw = download_data(caminho)
    tr = truncate_table(dw)
    up = upload_data(tr)

    branches >> [dw, end]
    dw >> tr >> up >> end

ibge_uf_update_task_flow()