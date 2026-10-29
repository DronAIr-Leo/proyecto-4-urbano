"""Carga única del histórico horario de calidad del aire 2016-2022 (CKAN Valencia)."""
import re
import sys
import time
import unicodedata
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE = Path(__file__).resolve().parent.parent
DESTINO = BASE / "data" / "raw" / "calidad_aire" / "historico"
CKAN = "https://opendata.vlci.valencia.es/api/3/action/datastore_search"
LOTE = 32000  # filas por petición

# Los dos recursos del portal CKAN (encontrados con package_search?fq=tags:aire)
RECURSOS = {
    "2016_2021": "786675f9-6cff-4fd5-9946-ccbc4d8c87c8",  # "Dades per hores qualitat aire 2016-2020"
    "2021_2022": "19b5a1a7-5888-4d3e-b69b-3c1436d64e6e",  # "Dades per hores qualitat aire 2021-2022"
}


def pedir(params):
    """datastore_search con 3 reintentos."""
    for intento in range(3):
        try:
            r = requests.get(CKAN, params=params, timeout=120)
            r.raise_for_status()
            datos = r.json()
            if not datos.get("success"):
                raise RuntimeError(datos.get("error"))
            return datos["result"]
        except (requests.RequestException, RuntimeError):
            if intento == 2:
                raise
            time.sleep(10 * (intento + 1))


def normalizar(nombre):
    """'PM2.5' -> 'pm2_5'; minúsculas, sin tildes ni espacios."""
    n = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", n).strip("_").lower()


def descargar(origen, resource_id, forzar):
    carpeta = DESTINO / f"origen={origen}"
    if any(carpeta.glob("*.parquet")) and not forzar:
        print(f"SALTO  {origen}: ya descargado (usa --forzar para repetir)")
        return
    carpeta.mkdir(parents=True, exist_ok=True)
    for viejo in carpeta.glob("*.parquet"):  # con --forzar, empezar limpio
        viejo.unlink()
    offset, parte, total = 0, 0, 0
    while True:
        res = pedir({"resource_id": resource_id, "limit": LOTE, "offset": offset})
        filas = res["records"]
        if not filas:
            break
        # Todos los valores como TEXTO, sin alterarlos (las fechas se arreglan en el Bloque 4)
        filas = [{normalizar(k): (None if v is None else str(v)) for k, v in f.items()}
                 for f in filas]
        pq.write_table(pa.Table.from_pylist(filas),
                       carpeta / f"parte_{parte:04d}.parquet", compression="snappy")
        total += len(filas)
        parte += 1
        offset += LOTE
        print(f"  {origen}: {total} / {res.get('total', '?')} filas")
    print(f"OK     {origen}: {total} filas en {parte} ficheros")


if __name__ == "__main__":
    forzar = "--forzar" in sys.argv
    for origen, rid in RECURSOS.items():
        descargar(origen, rid, forzar)