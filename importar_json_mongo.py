import json
import time
from pymongo import MongoClient

# --- Configuracion (segun docker-compose.yaml) ---
HOST        = 'localhost'
PORT        = 27018
USUARIO     = 'rootuser'
PASSWORD    = 'rootpassword'
DATABASE    = 'optimizacion'
CHUNK       = 5000

# --- Lista de archivos a subir y sus colecciones destino ---
TAREAS_IMPORTACION = [
    {"ruta": "D:/Big Data Aplicado/Optimizacion/Codigo/matchups128mb.json", "coleccion": "matchups_raw"},
    {"ruta": "mongo_optimizado.json", "coleccion": "matchups_norm"}
]

# --- Conexion ---
print('Conectando a MongoDB...')
client = MongoClient(f'mongodb://{USUARIO}:{PASSWORD}@{HOST}:{PORT}/?authSource=admin&authMechanism=SCRAM-SHA-256')
db = client[DATABASE]

# --- Bucle de importación ---
for tarea in TAREAS_IMPORTACION:
    JSON_PATH = tarea["ruta"]
    COLECCION = tarea["coleccion"]
    
    print(f"\nProcesando archivo: {JSON_PATH} -> Colección: {COLECCION}")
    col = db[COLECCION]
    
    # Limpiar solo esta colección específica
    col.drop()
    print(f'  Colección `{COLECCION}` preparada.')

    t0 = time.time()
    total = 0

    try:
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            datos = json.load(f)
    except FileNotFoundError:
        print(f"  ERROR: No se encontró el archivo {JSON_PATH}. Saltando...")
        continue

    # Aceptar tanto lista como objeto
    if isinstance(datos, dict):
        for val in datos.values():
            if isinstance(val, list):
                datos = val
                break
        else:
            datos = [datos]

    print(f'  Total documentos detectados: {len(datos):,}')

    lote = []
    for doc in datos:
        lote.append(doc)
        if len(lote) >= CHUNK:
            col.insert_many(lote)
            total += len(lote)
            lote = []
            print(f'    Insertados {total:,} documentos...', end='\r')

    if lote:
        col.insert_many(lote)
        total += len(lote)

    t1 = time.time()
    print(f'\n  Completado: {total:,} documentos en {t1 - t0:.2f}s')

client.close()
print(f'\n¡Toda la importación de MongoDB ha finalizado!')