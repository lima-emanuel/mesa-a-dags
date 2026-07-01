import hashlib
import os

import pandas as pd
import pendulum
import requests
import yaml
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
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

URL_RAIZ = (
    "https://app-hmg.cidades.gov.br/fluxo-residuos/web/site/download-planilha-geral"
)

FILE_NAME = "snis_lixao.xlsx"

lessonia_path = f"{LESSONIA_DATA_LOCAL}/SNIS"

DATABASE_URL = f"postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def get_sha():
    r = requests.get(URL_RAIZ, timeout=60)
    r.raise_for_status()

    os.makedirs(os.path.dirname(lessonia_path), exist_ok=True)

    with open(FILE_NAME, "wb") as f:
        f.write(r.content)

    with open(FILE_NAME, "rb") as f:
        new_sha = hashlib.file_digest(f, "sha256").hexdigest()

    return new_sha


@task.branch(task_id="check_dates")
def check_dates():
    sha = get_sha()

    if not sha:
        print("Not possible to extract the SHA256.")
        return "end"

    print("=== DEBUG LOGS UF ===")
    print(f"VALUE FROM WEBSITE (SNIS): {sha} | Type: {type(sha)}")

    engine = create_engine(DATABASE_URL)

    sql = "select distinct (last_modified) from public.snis_lixao"

    try:
        df_data = pd.read_sql(sql, con=engine)
        if not df_data.empty:
            last_sha = pd.to_datetime(df_data["sha"].iloc[0]).replace(tzinfo=None)
            print(f"VALUE FROM DB (POSTGRES): {last_sha} | Type: {type(last_sha)}")
            print("==================")

            if sha == last_sha:
                print("No new data on SNIS website.")
                return "end"
        else:
            print("Table is empty. Downloading.")
    except Exception as e:
        print(f"Error when consulting the database (table may not exist): {e}")

    return "download_data"


@task(task_id="download_data")
def download_data(destino_local):
    print("Initiating download of SNIS Lixão files")

    _ = get_sha()

    return destino_local


@task(task_id="truncate_table")
def truncate_table(caminho_arquivo):
    engine = create_engine(DATABASE_URL)
    print("Initiating TRUNCATE on table public.snis_lixao...")
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.snis_lixao;"))
        print("Table public.snis_lixao truncated succesfully!")
    except Exception as e:
        print(f"Error when truncating snis_lixao: {e}")

    return caminho_arquivo


@task(task_id="upload_data")
def upload_data(caminho):
    os.chdir(lessonia_path)

    df = pd.read_file(FILE_NAME)
    df["sha"] = get_sha()

    engine = create_engine(DATABASE_URL)

    df.to_postgis(name="snis_lixao", con=engine, if_exists="replace", index=False)
    print("Data inserted with success!")

    return caminho


@dag(
    dag_id="SNIS_LIXAO",
    schedule="0 0 1 * *",  # Roda às 11:00 UTC (uma hora após a de municípios)
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 17),
    tags=["daily", "transformation"],
    max_active_runs=1,
)
def snis_lixao_update_task_flow():
    branches = check_dates()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    dw = download_data(lessonia_path)
    tr = truncate_table(dw)
    up = upload_data(tr)

    branches >> [dw, end]
    dw >> tr >> up >> end


snis_lixao_update_task_flow()
