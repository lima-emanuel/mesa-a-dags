import os
import re
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

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

FILE_NAME = "dados_gerais_Brasil.kmz"
URL_RAIZ = "https://novoatlas.cepel.br/index.php/mapas-tematicos/"

lessonia_path = f"{LESSONIA_DATA_LOCAL}/inpe_ventos/"

DATABASE_URL = f"postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def get_last_modified():
    response = requests.get(URL_RAIZ, allow_redirects=True, timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    download_url = None

    def helper():
        label = soup.find(string=re.compile(r"Dados\s+Consolidados", re.I))
        if label:
            row = label.find_parent(["tr", "p", "div", "li"])
            if row:
                a = row.find("a", href=True)
                if a:
                    return urljoin(URL_RAIZ, a["href"])

    download_url = helper()
    if download_url is None:
        raise ValueError("Download URL not found on the webpage.")

    resp = requests.head(download_url, allow_redirects=True, timeout=30)
    resp.raise_for_status()

    last_modified = resp.headers.get("Last-Modified")

    if last_modified:
        return download_url, parsedate_to_datetime(last_modified)
    else:
        raise ValueError("Last-Modified header not found in the response.")


@task.branch(task_id="check_dates")
def check_dates():
    _, last_modified = get_last_modified()

    if not last_modified:
        print("Not possible to extract last modification date.")
        return "end"

    print("=== DEBUG LOGS UF ===")
    print(f"VALUE FROM WEBSITE (INPE): {last_modified} | Type: {type(last_modified)}")

    engine = create_engine(DATABASE_URL)

    sql = "select distinct (last_modified) from public.inpe_ventos"

    try:
        df_data = pd.read_sql(sql, con=engine)
        if not df_data.empty:
            last_date = pd.to_datetime(df_data["last_modified"].iloc[0]).replace(
                tzinfo=None
            )
            print(f"VALUE FROM DB (POSTGRES): {last_date} | Type: {type(last_date)}")
            print("==================")

            if last_modified.replace(tzinfo=None) <= last_date:
                print("No new data on INPE website.")
                return "end"
        else:
            print("Table is empty. Downloading.")
    except Exception as e:
        print(f"Error when consulting the database (table may not exist): {e}")

    return "download_data"


@task(task_id="download_data")
def download_data(destino_local):
    print("Initiating download of INPE Ventos files")

    os.makedirs(os.path.dirname(destino_local), exist_ok=True)

    download_url, _ = get_last_modified()

    response = requests.get(download_url, headers=HEADERS, stream=True, timeout=60)
    response.raise_for_status()
    with open(destino_local, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    return destino_local


@task(task_id="truncate_table")
def truncate_table(caminho_arquivo):
    engine = create_engine(DATABASE_URL)
    print("Initiating TRUNCATE on table public.inpe_ventos...")
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.inpe_ventos;"))
        print("Table public.inpe_ventos truncated succesfully!")
    except Exception as e:
        print(f"Error when truncating inpe_ventos: {e}")

    return caminho_arquivo


@task(task_id="upload_data")
def upload_data(caminho):
    download_url, last_modified = get_last_modified()

    df = gpd.read_file(f"{caminho}/{download_url.split('/')[-1]}", driver="KMZ")
    df["last_modified"] = last_modified

    engine = create_engine(DATABASE_URL)

    df.to_postgis(name="inpe_ventos", con=engine, if_exists="replace", index=False)
    print("Data inserted with success!")


@dag(
    dag_id="INPE_VENTOS",
    schedule="0 11 * * *",  # Roda às 11:00 UTC (uma hora após a de municípios)
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 17),
    tags=["daily", "transformation"],
    max_active_runs=1,
)
def inpe_ventos_update_task_flow():
    branches = check_dates()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    dw = download_data(lessonia_path)
    tr = truncate_table(dw)
    up = upload_data(tr)

    branches >> [dw, end]
    dw >> tr >> up >> end


inpe_ventos_update_task_flow()
