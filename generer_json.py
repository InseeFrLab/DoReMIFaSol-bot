import requests
import json
import os
import s3fs
import re
import zipfile
from time import sleep
import pandas as pd
import hashlib
import sys
import io
import csv

s3 = s3fs.S3FileSystem(
    endpoint_url=os.getenv("S3_ENDPOINT"),
    key=os.getenv("ACCESS_KEY"),
    secret=os.getenv("SECRET_KEY"),
)

url = "https://api.insee.fr/melodi/catalog/all"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/129.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

try:
    requete = requests.get(url)
    requete.raise_for_status()
    catalog = json.loads(requete.content)
except Exception as e:
    print(f"Erreur : {e}")
    sys.exit(1)


def select_langue(libelles, langue):
    return [libelle for libelle in libelles if libelle["lang"] == langue][0]["content"]


def isZip(product):
    if "packageFormat" in product and re.match(r".*zip.*", product["packageFormat"]):
        return True
    else:
        return False

def telechargeProduit(url):
    stop = False
    while not stop:
        try:
            dl_produit = requests.get(url, headers=headers)
            dl_produit.raise_for_status()
            stop = True
            return dl_produit
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors du téléchargement : {e}")
            if dl_produit.status_code == 429:
                sleep(30)
                continue
            else:
                return None
                break
        except requests.exceptions.Timeout:
            print("Timeout. Nouvelle tentative dans 1 mn.")
            sleep(60)
            continue

res_0 = [
    {
        "nom": item["identifier"].upper(),
        "libelle": select_langue(item["title"], "fr"),
        "collection": select_langue(item["theme"][-1]["label"], "fr"),
        "lien": f"https://api.insee.fr/melodi/file/{item['identifier']}/{item['identifier']}_CSV_FR",
        "type": "csv",
        "separateur": ";",
        "zip": True,
        "big_zip": False,
        "fichier_donnees": f"{item['identifier']}_data.csv",
        "fichier_meta": f"{item['identifier']}_metadata.csv",
        "api_rest": False,
        "date_parution": item["issued"],
        "date_modification": item["modified"] if "modified" in item else None,
        "url_documentation": item["relations"][-1] if "relations" in item else None,
        "melodi": True
    }
    for item in catalog
]

for r in res_0:
    if r["url_documentation"] is None:
        del r["url_documentation"]
    dl_item = requests.get(r["lien"], headers=headers)
    while dl_item.status_code == 429:
        sleep(30)
        dl_item = requests.get(r["lien"], headers=headers)
    r["disponible"] = dl_item.status_code == 200
    if dl_item.status_code == 200:
        r["size"] = len(dl_item.content)
        r["md5"] = hashlib.md5(dl_item.content).hexdigest()
        with zipfile.ZipFile(io.BytesIO(dl_item.content)) as zf:
            if f"{r['nom']}_metadata.csv" in zf.namelist():
                with zf.open(f"{r['nom']}_metadata.csv") as metadata:
                    reader = csv.DictReader(io.TextIOWrapper(metadata, encoding='utf-8'), delimiter=';')
                    codvar_to_libvar = {row['COD_VAR']: row['LIB_VAR'] for row in reader}
                r["label_col"] = codvar_to_libvar

res = [
    {
        "nom": product["id"].upper(),
        "date_ref": item["temporal"]["startPeriod"].split("T")[0],
        "libelle": product["title"],
        "collection": select_langue(item["theme"][-1]["label"], "fr"),
        "lien": product["accessURL"],
        "type": product["format"].lower(),
        "zip": isZip(product),
        "api_rest": False,
        "size": product["byteSize"],
        "date_parution": product["issued"],
        "date_modification": product["modified"] if "modified" in item else None,
        "url_documentation": item["relations"][-1] if "relations" in item else None,
        "melodi": True
    }
    for item in catalog
    if "product" in item
    for product in item["product"]
]

for r in res:
    if r["url_documentation"] is None:
        del r["url_documentation"]
    print(f"Téléchargement de {r['nom']}...")
    dl_produit = telechargeProduit(r["lien"])
    if r["zip"]:
        nomProduit = f"{r['nom']}.zip"
        if dl_produit:
            with open(nomProduit, "wb") as f:
                f.write(dl_produit.content)
            with zipfile.ZipFile(nomProduit, "r") as zf:
                liste_fichiers = zf.namelist()
                r["big_zip"] = sum(
                    [file_info.file_size for file_info in zf.infolist()]
                ) > 4 * (2**30)
            r["fichier_donnees"] = [
                fichier
                for fichier in liste_fichiers
                if not re.match(r".*metadata.*", fichier)
            ][0]
            r["fichier_meta"] = [
                fichier
                for fichier in liste_fichiers
                if re.match(r".*metadata.*", fichier)
            ][0]
            if r["type"] not in ["csv", "xls", "xlsx"]:
                r["type"] = r["fichier_donnees"].split(".")[-1]
            if r["type"] == "csv":
                r["separateur"] = ";"
                with zipfile.ZipFile(nomProduit, "r") as zf:
                    with zf.open(r["fichier_meta"], "r") as f:
                        metadata = pd.read_csv(f, sep=";")
                liste_libelles = metadata.drop_duplicates(
                    subset=["COD_VAR", "LIB_VAR"]
                )
                r["label_col"] = dict(
                    zip(liste_libelles["COD_VAR"], liste_libelles["LIB_VAR"])
                )
    else:
        nomProduit = f"{r['nom']}.{r['type']}"
        if dl_produit:
            with open(nomProduit, "wb") as f:
                f.write(dl_produit.content)
        if r["type"] == "xlsx":
            r["premiere_ligne"] = 4
            r["onglet"] = "__MELODI__"
    if dl_produit:
        with open(nomProduit, "rb") as f:
            r["md5"] = hashlib.md5(f.read()).hexdigest()
        os.remove(nomProduit)
        r["disponible"] = True
    else:
        r["disponible"] = False
        if r["zip"]:
            r["big_zip"] = False


with s3.open("pierrelamarche/melodi/liste_donnees.json", "w", encoding="utf-8") as f:
    json.dump(res_0+res, f, indent=4, ensure_ascii=False)
