import io
import os

import geopandas as gpd
import pandas as pd
import pendulum
import rarfile
import yaml
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
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

FILE_NAME = "rel_mig4.rar"

SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

lessonia_path = f"{LESSONIA_DATA_LOCAL}/{FILE_NAME}"

DATABASE_URL = f"postgresql://{LESSONIA_USER}:{LESSONIA_PASS}@{LESSONIA_HOST}:{LESSONIA_PORT}/{LESSONIA_NAME}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def get_last_modified():
    """
    Return the Google Drive 'Modification Time' date as a pendulum datetime.
    """
    creds = None

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        print("Credentials not available or invalid.")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    else:
        raise
    with open("token.json", "w") as token:
        token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)

        results = (
            service.files()
            .list(
                q=f"'{FOLDER_ID}' in parents and name = '{FILE_NAME}' and trashed = false",
                spaces="drive",
                fields="files(id, name)",
            )
            .execute()
        )
        files = results.get("files", [])
        if not files:
            print("No matching file found.")
        else:
            file_id = files[0]["id"]

        metadata = (
            service.files()
            .get(fileId=file_id, fields="id, name, modifiedTime, mimeType, size")
            .execute()
        )
        modified_str = metadata["modifiedTime"]
        modified_dt = pendulum.parse(modified_str)
    except HttpError as error:
        print(f"An error occurred: {error}")

    return modified_dt, service, file_id


@task.branch(task_id="check_dates")
def check_dates():
    last_modified, _, _ = get_last_modified()

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

    _, service, file_id = get_last_modified()

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(destino_local, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"Download {int(status.progress() * 100)}%.")

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
    last_modified, _, _ = get_last_modified()

    pasta_extracao = os.path.join(os.path.dirname(caminho), "extracao_icmbio_aves")
    os.makedirs(pasta_extracao, exist_ok=True)

    with rarfile.RarFile(caminho, "r") as rf:
        rf.extractall(path=pasta_extracao)

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

    df.to_postgis(name="icmbio_aves", con=engine, if_exists="replace", index=False)
    print("Data inserted with success!")


@dag(
    dag_id="ICMBIO_AVES",
    schedule="0 11 * * *",  # Roda às 11:00 UTC (uma hora após a de municípios)
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
