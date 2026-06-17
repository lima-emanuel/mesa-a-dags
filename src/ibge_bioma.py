import os
import zipfile
from pathlib import Path

import geopandas as gpd
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

URL_RAIZ = "https://www.ibge.gov.br/geociencias/cartas-e-mapas/informacoes-ambientais/15842-biomas.html"

FILE_NAME = "dmRgNtTrBmBr_v2025_vetor_e250K.zip"

URL_ZIP = f"https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/dominios_regioes_naturais_do_brasil/2025/dados_geoespaciais/{FILE_NAME}"

lessonia_path = f"{LESSONIA_DATA_LOCAL}/{FILE_NAME}"

DATABASE_URL = f"postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def get_last_modified():
    dbf_files = download_and_extract_helper(lessonia_path)

    dbf_path = Path(dbf_files[0])
    with dbf_path.open("rb") as f:
        header = f.read(4)  # byte 0 = version, bytes 1-3 = YY MM DD
    if len(header) < 4:
        raise ValueError("DBF header too short")

    # bytes 1-3 are already single-byte integers; no need for hex, but we can use them directly
    year_byte = header[1]
    month_byte = header[2]
    day_byte = header[3]

    year = 1900 + year_byte  # per DBF specification[web:25][web:28]
    month = month_byte
    day = day_byte

    return pendulum.datetime(year, month, day)


def download_and_extract_helper(path):
    response = requests.get(URL_ZIP, headers=HEADERS, stream=True, timeout=60)
    response.raise_for_status()
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    pasta_extracao = os.path.join(os.path.dirname(path), "extracao_ibge_bioma")
    os.makedirs(pasta_extracao, exist_ok=True)

    with zipfile.ZipFile(path, "r") as zip_ref:
        zip_ref.extractall(pasta_extracao)

    dbf_files = [
        os.path.join(root, name)
        for root, dirs, files in os.walk(pasta_extracao)
        for name in files
        if name.endswith(".dbf")
    ]

    if not dbf_files:
        raise FileNotFoundError("No .shp file found on the extracted ZIP.")

    return dbf_files


@task.branch(task_id="check_dates")
def check_dates():
    last_modified = get_last_modified()

    if not last_modified:
        print("Not possible to extract last modification date.")
        return "end"

    print("=== DEBUG LOGS UF ===")
    print(f"VALUE FROM WEBSITE (CPRM): {last_modified} | Type: {type(last_modified)}")

    engine = create_engine(DATABASE_URL)

    sql = "select distinct (last_modified) from public.ibge_bioma"

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
    print("Initiating download of IBGE Bioma files")

    os.makedirs(os.path.dirname(destino_local), exist_ok=True)

    response = requests.get(URL_ZIP, headers=HEADERS, stream=True, timeout=60)
    response.raise_for_status()
    with open(destino_local, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    return destino_local


@task(task_id="truncate_table")
def truncate_table(caminho_arquivo):
    engine = create_engine(DATABASE_URL)
    print("Initiating TRUNCATE on table public.ibge_bioma...")
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.ibge_bioma;"))
        print("Table public.ibge_bioma truncated succesfully!")
    except Exception as e:
        print(f"Error when truncating ibge_bioma: {e}")

    return caminho_arquivo


@task(task_id="upload_data")
def upload_data(caminho):
    pasta_extracao = os.path.join(os.path.dirname(caminho), "extracao_ibge_bioma")
    os.makedirs(pasta_extracao, exist_ok=True)

    with zipfile.ZipFile(caminho, "r") as zip_ref:
        zip_ref.extractall(pasta_extracao)

    last_modified = get_last_modified()

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

    df.to_postgis(name="ibge_bioma", con=engine, if_exists="replace", index=False)
    print("Data inserted with success!")


@dag(
    dag_id="ibge_bioma",
    schedule="0 11 * * 1",  # Roda às 11:00 UTC (uma hora após a de municípios)
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 17),
    tags=["daily", "transformation"],
    max_active_runs=1,
)
def ibge_bioma_update_task_flow():
    branches = check_dates()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    dw = download_data(lessonia_path)
    tr = truncate_table(dw)
    up = upload_data(tr)

    branches >> [dw, end]
    dw >> tr >> up >> end


ibge_bioma_update_task_flow()
