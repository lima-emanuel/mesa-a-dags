import datetime
import os
import shutil
import subprocess
from pathlib import Path

import gdown
import geopandas as gpd
import pandas as pd
import pendulum
import rarfile
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

FOLDER_ID = "109XnahlTtYhY9ycSj-RrOV_RTq-RjhB8"
URL_RAIZ = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
URL_RAR = (
    "https://drive.google.com/uc?id=1fJs20EAqV_KXlnswzJuM1Xdywl4pBiRI&export=download"
)


FILE_NAME = "rel_mig4.rar"

SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

lessonia_path = f"{LESSONIA_DATA_LOCAL}"

DATABASE_URL = f"postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def get_last_modified():
    """
    Return the rar file 'Modification Time' date as a pendulum datetime.
    """
    os.chdir(lessonia_path)
    gdown.download(URL_RAR, FILE_NAME, quiet=False)

    path = Path(FILE_NAME)

    stat = path.stat()
    return datetime.fromtimestamp(stat.st_mtime)


@task.branch(task_id="check_dates")
def check_dates():
    last_modified = get_last_modified()

    if not last_modified:
        print("Not possible to extract last modification date.")
        return "end"

    print("=== DEBUG LOGS UF ===")
    print(f"VALUE FROM WEBSITE (ICMBio): {last_modified} | Type: {type(last_modified)}")

    engine = create_engine(DATABASE_URL)

    sql = "select distinct (last_modified) from public.icmbio_aves"

    try:
        df_data = pd.read_sql(sql, con=engine)
        if not df_data.empty:
            last_date = pd.to_datetime(df_data["last_modified"].iloc[0]).replace(
                tzinfo=None
            )
            print(f"VALUE FROM DB (POSTGRES): {last_date} | Type: {type(last_date)}")
            print("==================")

            if last_modified.replace(tzinfo=None) <= last_date:
                print("No new data on ICMBios website.")
                return "end"
        else:
            print("Table is empty. Downloading.")
    except Exception as e:
        print(f"Error when consulting the database (table may not exist): {e}")

    return "download_data"


@task(task_id="download_data")
def download_data(destino_local):
    print("Initiating download of ICMBio Aves files")

    _ = get_last_modified()

    print("Download complete.")

    return destino_local


@task(task_id="truncate_table")
def truncate_table(caminho_arquivo):
    engine = create_engine(DATABASE_URL)
    print("Initiating TRUNCATE on table public.icmbio_aves...")
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.icmbio_aves;"))
        print("Table public.icmbio_aves truncated succesfully!")
    except Exception as e:
        print(f"Error when truncating icmbio_aves: {e}")

    return caminho_arquivo


@task(task_id="upload_data")
def upload_data(caminho):
    last_modified = get_last_modified()

    pasta_extracao = os.path.join(os.path.dirname(caminho), "extracao_icmbio_aves")
    os.makedirs(pasta_extracao, exist_ok=True)

    if shutil.which("tar"):
        print("Using system 'tar' command to extract archive...")
        try:
            subprocess.run(
                ["tar", "xf", f"{caminho}/{FILE_NAME}", "--directory", pasta_extracao],
                check=True,
            )
            print("Extracted successfully using system 'tar'.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"System 'tar' failed to extract archive: {e}")
    else:
        print(
            "System 'tar' not found. Falling back to standard Python 'zipfile' module..."
        )
        with rarfile.RarFile(f"{caminho}/{FILE_NAME}", "r") as rf:
            rf.extractall(path=pasta_extracao)

    arquivos_shp = [
        os.path.join(root, name)
        for root, dirs, files in os.walk(pasta_extracao)
        for name in files
        if name.endswith(".shp")
    ]

    if not arquivos_shp:
        raise FileNotFoundError("No .shp file found on the extracted ZIP.")

    for shapefile in arquivos_shp:
        df = gpd.read_file(shapefile)
        df["last_modified"] = last_modified

        engine = create_engine(DATABASE_URL)

        df.to_postgis(
            name=f"icmbio_aves_{shapefile[5:-4]}",
            con=engine,
            if_exists="replace",
            index=False,
        )
        print("Data inserted with success!")


@dag(
    dag_id="ICMBIO_AVES",
    schedule="0 0 1 * *",
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 17),
    tags=["daily", "transformation"],
    max_active_runs=1,
)
def icmbio_aves_update_task_flow():
    branches = check_dates()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    dw = download_data(lessonia_path)
    tr = truncate_table(dw)
    up = upload_data(tr)

    branches >> [dw, end]
    dw >> tr >> up >> end


icmbio_aves_update_task_flow()
