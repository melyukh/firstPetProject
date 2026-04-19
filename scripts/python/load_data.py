import os
import codecs
import zipfile
import requests
import io
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/.env")
DATA_DIR = os.getenv("DATA_DIR")

DOWNLOAD_API = requests.get("https://cloud-api.yandex.net/v1/disk/public/resources/download?",
    params = {"public_key": "https://disk.yandex.ru/d/bhf2M8C557AFVw"}
    ).json()["href"]

print(DOWNLOAD_API)

outer_zip_path = f"{DATA_DIR}/archive.zip"
with requests.get(DOWNLOAD_API, stream=True) as r:
    r.raise_for_status()
    with open(outer_zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
print("Архив скачан")

# Распаковать внешний zip
with zipfile.ZipFile(outer_zip_path, 'r') as outer_zip:
    inner_zip_name = next(f for f in outer_zip.namelist() if f.endswith('.zip'))
    outer_zip.extract(inner_zip_name, DATA_DIR)
print("Внешний архив распакован")

# Распаковать внутренний zip
inner_zip_path = f"{DATA_DIR}/{inner_zip_name}"
with zipfile.ZipFile(inner_zip_path, 'r') as inner_zip:
    inner_zip.extractall(DATA_DIR)
    print("Разархивировано, имя файла:", inner_zip.namelist()[0])

# Удалить временные архивы
os.remove(outer_zip_path)
os.remove(inner_zip_path)

with codecs.open(f"{DATA_DIR}/russian_houses.csv", "r", "utf-16le") as f_in:
    with codecs.open(f"{DATA_DIR}/russian_houses_utf_8.csv", "w", "utf-8") as f_out:
        while True:
            chunk = f_in.read(1024 * 1024)
            if not chunk:
                break
            f_out.write(chunk)

print("файл перекодирован из utf-16le в utf-8")