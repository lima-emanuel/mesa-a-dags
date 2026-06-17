import os
import re
import zipfile

import geopandas as gpd
import pandas as pd
import pendulum
import requests
import yaml
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

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

now = pendulum.now()

LAST_CENSUS = 2022
NEXT_CENSUS = 2030

# URL Raiz para Malhas de Setores Censitários (Brasil)
URL_RAIZ_IBGE = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/setores/shp/BR/"

if now.year > NEXT_CENSUS:
    zip_name = "BR_setores_CD{NEXT_CENSUS}.zip"
else:
    zip_name = "BR_setores_CD{LAST_CENSUS}.zip"

lessonia_path = f"{LESSONIA_DATA_LOCAL}/{zip_name}"
url_file_path = f"{URL_RAIZ_IBGE}/{zip_name}"

DATABASE_URL = f"postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def get_last_modified(url_raiz):
    """
    Searches the IBGE webpage for the modification date of BR_setores_CD2022.zip
    and returns it.
    """
    try:
        if not url_raiz.endswith("/"):
            url_raiz += "/"

        res_raiz = requests.get(url_raiz, headers=HEADERS, timeout=60)
        if res_raiz.status_code != 200:
            return None, None

        soup_raiz = BeautifulSoup(res_raiz.text, "html.parser")
        table = soup_raiz.find("table")

        last_modified = None

        pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}"

        for row in table.find_all("tr"):
            for cell in row.find_all("td"):
                text = cell.get_text(" ", strip=True)
                m = re.search(pattern, text)
                if m:
                    last_modified = pendulum.parse(m.group())

        if last_modified:
            print(
                f"[IBGE SETORES CENSITÁRIOS] Last modified date in website: {last_modified}"
            )
            return last_modified

        print(
            "[IBGE SETORES CENSITÁRIOS] Not possible to extract last modification date."
        )
        return None, None

    except Exception as e:
        print(f"Failure to scrap the IBGE website: {e}")
        return None, None


@task.branch(task_id="check_dates")
def check_dates():
    last_modified = get_last_modified(URL_RAIZ_IBGE)

    if not last_modified:
        print("Not possible to extract last modification date.")
        return "end"

    print("=== DEBUG LOGS UF ===")
    print(f"VALUE FROM WEBSITE (IBGE): {last_modified} | Type: {type(last_modified)}")

    engine = create_engine(DATABASE_URL)

    sql = "select distinct (last_modified) from public.ibge_censitarios"

    try:
        df_data = pd.read_sql(sql, con=engine)
        if not df_data.empty:
            last_date = pd.to_datetime(df_data["last_modified"].iloc[0]).replace(
                tzinfo=None
            )
            print(f"VALUE FROM DB (POSTGRES): {last_date} | Type: {type(last_date)}")
            print("==================")

            if last_modified.replace(tzinfo=None) <= last_date:
                print("No new data on IBGEs website.")
                return "end"
        else:
            print("Table is empty. Downloading.")
    except Exception as e:
        print(f"Error when consulting the database (table may not exist): {e}")

    return "download_data"


@task(task_id="download_data")
def download_data(destino_local):
    print(f"Initiating download of file: {url_file_path}")
    response = requests.get(url_file_path, headers=HEADERS, stream=True, timeout=60)

    if response.status_code != 200:
        raise ConnectionError(
            f"Not possible to download. Status: {response.status_code}"
        )

    os.makedirs(os.path.dirname(destino_local), exist_ok=True)

    with open(destino_local, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    return destino_local


@task(task_id="truncate_table")
def truncate_table(caminho_arquivo):
    engine = create_engine(DATABASE_URL)
    print("Initiating TRUNCATE on table public.ibge_censitarios...")
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.ibge_censitarios;"))
        print("Table public.ibge_censitarios truncated succesfully!")
    except Exception as e:
        print(f"Error when truncating ibge_censitarios: {e}")

    return caminho_arquivo


@task(task_id="upload_data")
def upload_data(caminho):
    last_modified = get_last_modified(URL_RAIZ_IBGE)

    pasta_extracao = os.path.join(os.path.dirname(caminho), "extracao_ibge_censitarios")
    os.makedirs(pasta_extracao, exist_ok=True)

    with zipfile.ZipFile(caminho, "r") as zip_ref:
        zip_ref.extractall(pasta_extracao)

    arquivos_shp = [
        os.path.join(root, name)
        for root, dirs, files in os.walk(pasta_extracao)
        for name in files
        if name.endswith(".shp")
    ]

    if not arquivos_shp:
        raise FileNotFoundError("No .shp file found on the extracted ZIP.")

    df = gpd.read_file(arquivos_shp[0])
    df["last_modified"] = last_modified

    engine = create_engine(DATABASE_URL)

    df.to_postgis(name="ibge_censitarios", con=engine, if_exists="replace", index=False)
    print("Data inserted with success!")


@dag(
    dag_id="IBGE_UF",
    schedule="0 11 * * *",  # Roda às 11:00 UTC (uma hora após a de municípios)
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 17),
    tags=["daily", "transformation"],
    max_active_runs=1,
)
def ibge_censitarios_update_task_flow():
    branches = check_dates()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    dw = download_data(lessonia_path)
    tr = truncate_table(dw)
    up = upload_data(tr)

    branches >> [dw, end]
    dw >> tr >> up >> end


ibge_censitarios_update_task_flow()
