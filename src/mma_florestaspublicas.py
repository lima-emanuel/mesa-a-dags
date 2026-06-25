import os
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

URL_RAIZ = "http://mapas.mma.gov.br/i3geo/datadownload.htm?florestaspublicas"
URL_GERACAO = "http://mapas.mma.gov.br/i3geo/classesphp/mapa_controle.php?map_file=&funcao=download3&tema=florestaspublicas"
URL_SHP = "http://mapas.mma.gov.br/ms_tmp/florestaspublicas.shp"
URL_SHX = "http://mapas.mma.gov.br/ms_tmp/florestaspublicas.shx"
URL_DBF = "http://mapas.mma.gov.br/ms_tmp/florestaspublicas.dbf"

lessonia_path = f"{LESSONIA_DATA_LOCAL}/florestaspublicas/"

DATABASE_URL = f"postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def get_last_modified(dbf_path):
    """
    Return the DBF 'last update' date as a pendulum datetime (YYYY-MM-DD, time set to 00:00).
    Raises if the bytes are not a valid date.
    """
    dbf_path = Path(dbf_path)
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


def get_last_modified_from_stream(dbf_url):
    session = requests.Session()
    session.headers.update(HEADERS)

    # Trigger generation of shapefiles on the server
    response = session.get(URL_GERACAO, timeout=60)
    response.raise_for_status()

    res = session.get(URL_DBF, stream=True, timeout=60)
    res.raise_for_status()
    res.raw.decode_content = True  # handle gzip/deflate if needed
    header = res.raw.read(4)

    year_byte = header[1]
    month_byte = header[2]
    day_byte = header[3]

    year = 1900 + year_byte  # per DBF specification[web:25][web:28]
    month = month_byte
    day = day_byte

    return pendulum.datetime(year, month, day)


@task.branch(task_id="check_dates")
def check_dates():
    last_modified = get_last_modified_from_stream(URL_DBF)

    if not last_modified:
        print("Not possible to extract last modification date.")
        return "end"

    print("=== DEBUG LOGS UF ===")
    print(f"VALUE FROM WEBSITE (MMA): {last_modified} | Type: {type(last_modified)}")

    engine = create_engine(DATABASE_URL)

    sql = "select distinct (last_modified) from public.mma_florestaspublicas"

    try:
        df_data = pd.read_sql(sql, con=engine)
        if not df_data.empty:
            last_date = pd.to_datetime(df_data["last_modified"].iloc[0]).replace(
                tzinfo=None
            )
            print(f"VALUE FROM DB (POSTGRES): {last_date} | Type: {type(last_date)}")
            print("==================")

            if last_modified.replace(tzinfo=None) <= last_date:
                print("No new data on MMAs website.")
                return "end"
        else:
            print("Table is empty. Downloading.")
    except Exception as e:
        print(f"Error when consulting the database (table may not exist): {e}")

    return "download_data"


@task(task_id="download_data")
def download_data(destino_local):
    print("Initiating download of floresta pública files")

    os.makedirs(os.path.dirname(destino_local), exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    # Trigger generation of shapefiles on the server
    response = session.get(URL_GERACAO, timeout=60)
    response.raise_for_status()

    response = session.get(URL_SHP, stream=True, timeout=60)
    response.raise_for_status()
    with open(f"{destino_local}/florestaspublicas.shp", "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    response = session.get(URL_SHX, stream=True, timeout=60)
    response.raise_for_status()
    with open(f"{destino_local}/florestaspublicas.shx", "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    response = session.get(URL_DBF, stream=True, timeout=60)
    response.raise_for_status()
    with open(f"{destino_local}/florestaspublicas.dbf", "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    return destino_local


@task(task_id="truncate_table")
def truncate_table(caminho_arquivo):
    engine = create_engine(DATABASE_URL)
    print("Initiating TRUNCATE on table public.mma_florestaspublicas...")
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.mma_florestaspublicas;"))
        print("Table public.mma_florestaspublicas truncated succesfully!")
    except Exception as e:
        print(f"Error when truncating mma_florestaspublicas: {e}")

    return caminho_arquivo


@task(task_id="upload_data")
def upload_data(caminho):
    last_modified = get_last_modified(f"{caminho}/florestaspublicas.dbf")

    arquivo_shp = f"{caminho}/florestaspublicas.shp"

    if not arquivo_shp:
        raise FileNotFoundError("No .shp file found.")

    df = gpd.read_file(arquivo_shp)
    df["last_modified"] = last_modified

    engine = create_engine(DATABASE_URL)

    df.to_postgis(
        name="mma_florestaspublicas", con=engine, if_exists="replace", index=False
    )
    print("Data inserted with success!")


@dag(
    dag_id="MMA_FP",
    schedule="0 11 * * *",  # Roda às 11:00 UTC (uma hora após a de municípios)
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 17),
    tags=["daily", "transformation"],
    max_active_runs=1,
)
def mma_florestaspublicas_update_task_flow():
    branches = check_dates()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    dw = download_data(lessonia_path)
    tr = truncate_table(dw)
    up = upload_data(tr)

    branches >> [dw, end]
    dw >> tr >> up >> end


mma_florestaspublicas_update_task_flow()
