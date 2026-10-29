"""Recolector P4: guarda una foto de tráfico, aire y meteo de Valencia en Parquet."""
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

# Rutas basadas en la ubicación del script (así funciona igual desde el Programador de tareas)
BASE = Path(__file__).resolve().parent.parent
DATA_DIR = BASE / "data" / "raw"
load_dotenv(BASE / ".env")

GEOPORTAL = "https://geoportal.valencia.es/server/rest/services/OPENDATA"

FUENTES = {
    "espiras": {"capa": "Trafico/MapServer/208", "ruta": "trafico/espiras"},
    "tramos": {"capa": "Trafico/MapServer/188", "ruta": "trafico/tramos"},
    "estado": {"capa": "Trafico/MapServer/192", "ruta": "trafico/estado"},
    "aire": {"capa": "MedioAmbiente/MapServer/156", "ruta": "calidad_aire/estaciones"},
    "meteo": {"capa": "MedioAmbiente/MapServer/157", "ruta": "meteo/municipal"},
}

AEMET_URL = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/46250"


# ---------- 1. Pedir datos con reintentos ----------
def get_con_reintentos(url, params=None, headers=None, reintentos=3):
    """GET con reintentos ante errores de red, 429 y 5xx."""
    for intento in range(reintentos):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if intento == reintentos - 1:
                raise
            time.sleep(10 * (intento + 1))


# ---------- 2. Descargar una capa del Geoportal (con paginación) ----------
def descargar_capa(capa):
    url = f"{GEOPORTAL}/{capa}/query"
    params = {"where": "1=1", "outFields": "*", "f": "json", "resultOffset": 0}
    registros = []
    while True:
        datos = get_con_reintentos(url, params).json()
        if "error" in datos:
            raise RuntimeError(f"ArcGIS: {datos['error']}")
        registros += [f["attributes"] for f in datos["features"]]
        if not datos.get("exceededTransferLimit"):
            return registros
        params["resultOffset"] = len(registros)


# ---------- 3. AEMET: dos pasos ----------
def descargar_aemet():
    clave = os.getenv("AEMET_API_KEY")
    if not clave:
        raise RuntimeError("Falta AEMET_API_KEY en el .env")
    # Paso 1: AEMET responde con la URL donde están los datos
    paso1 = get_con_reintentos(AEMET_URL, headers={"api_key": clave}).json()
    if paso1.get("estado") != 200:
        raise RuntimeError(f"AEMET: {paso1.get('descripcion')}")
    # Paso 2: descargar esa segunda URL
    prediccion = get_con_reintentos(paso1["datos"]).json()
    filas = []
    for dia in prediccion[0]["prediccion"]["dia"]:
        filas.append({
            "fecha": dia["fecha"][:10],
            "temperatura_maxima": dia["temperatura"]["maxima"],
            "temperatura_minima": dia["temperatura"]["minima"],
            "raw": json.dumps(dia, ensure_ascii=False),  # el día completo, por si acaso
        })
    return filas


# ---------- 4. Guardar en Parquet y registrar ----------
def guardar_parquet(registros, ruta, nombre, captura):
    """Escribe data/raw/<ruta>/fecha=AAAA-MM-DD/<nombre>_AAAAMMDDTHHMMSSZ.parquet"""
    tabla = pa.Table.from_pylist(registros)
    tabla = tabla.append_column("captura_utc", pa.array([captura] * tabla.num_rows))
    carpeta = DATA_DIR / ruta / f"fecha={captura:%Y-%m-%d}"
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / f"{nombre}_{captura:%Y%m%dT%H%M%SZ}.parquet"
    pq.write_table(tabla, destino, compression="snappy")
    return destino


def registrar(captura, fuente, ok, filas, error=""):
    """Añade una línea a data/raw/_registro/ejecuciones.csv"""
    fichero = DATA_DIR / "_registro" / "ejecuciones.csv"
    fichero.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not fichero.exists()
    with fichero.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nuevo:
            w.writerow(["captura_utc", "fuente", "ok", "filas", "error"])
        w.writerow([captura.isoformat(), fuente, int(ok), filas, error[:300]])


# ---------- 5. Punto de entrada: cada fuente, independiente ----------
def recoger(nombre, funcion, ruta, captura):
    try:
        registros = funcion()
        destino = guardar_parquet(registros, ruta, nombre, captura)
        registrar(captura, nombre, True, len(registros))
        print(f"OK    {nombre}: {len(registros)} filas -> {destino.name}")
        return True
    except Exception as e:  # un fallo aquí no para a las demás fuentes
        registrar(captura, nombre, False, 0, str(e))
        print(f"ERROR {nombre}: {e}")
        return False


def main(fuentes):
    captura = datetime.now(timezone.utc).replace(microsecond=0)
    resultados = []
    for nombre in fuentes:
        if nombre == "aemet":
            resultados.append(recoger("aemet", descargar_aemet, "meteo/aemet", captura))
        else:
            cfg = FUENTES[nombre]
            resultados.append(recoger(nombre, lambda c=cfg["capa"]: descargar_capa(c), cfg["ruta"], captura))
    return 0 if all(resultados) else 1


if __name__ == "__main__":
    # Sin argumentos: todas. Con argumentos: solo esas (ej.: python recolector.py estado)
    elegidas = sys.argv[1:] or list(FUENTES) + ["aemet"]
    sys.exit(main(elegidas))