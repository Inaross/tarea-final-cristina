import pandas as pd
import numpy as np  # <-- Necesario para detectar y reemplazar los NaN
import json
import time

# --- 1. CONFIGURACIÓN Y CARGA ---
# Pon aquí el nombre exacto de tu archivo CSV ya reducido
ARCHIVO_CSV_REDUCIDO = "D:\Big Data Aplicado\Optimizacion\Codigo\matchups128mb.csv"

print(f"📥 Cargando datos desde {ARCHIVO_CSV_REDUCIDO}...")
df = pd.read_csv(ARCHIVO_CSV_REDUCIDO)

# --- 1.5 LIMPIEZA DE DATOS (CORRECCIÓN JSON) ---
print("🧹 Limpiando datos: eliminando columnas fantasma y corrigiendo nulos...")
# Elimina cualquier columna que empiece por "Unnamed"
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
# Reemplaza los NaN por None para que el JSON exporte "null" válidos
df = df.replace({np.nan: None})


# --- 2. NORMALIZACIÓN PARA MYSQL (Genera 3 archivos CSV) ---
print("\n⚙️ Generando archivos relacionales para MySQL...")

# Tabla 1: Invocadores (Usuarios únicos)
df_invocadores = df[['PUUID', 'SUMMONERNAME']].drop_duplicates().reset_index(drop=True)
df_invocadores.to_csv("mysql_1_invocadores.csv", index=False, encoding='utf-8')
print(f"  ✓ Creado: mysql_1_invocadores.csv ({len(df_invocadores)} registros)")

# Tabla 2: Partidas (Datos únicos de la partida)
df_partidas = df[['P_MATCH_ID', 'GAMEVERSION']].drop_duplicates().reset_index(drop=True)
df_partidas.to_csv("mysql_2_partidas.csv", index=False, encoding='utf-8')
print(f"  ✓ Creado: mysql_2_partidas.csv ({len(df_partidas)} registros)")

# Tabla 3: Detalles/Estadísticas (Tabla intermedia que une todo)
columnas_stats = ['P_MATCH_ID', 'PUUID', 'CHAMPION', 'WIN', 'KILLS', 'DEATHS', 
                  'ASSISTS', 'GOLDEARNED', 'TOTALMINIONSKILLED', 'VISIONSCORE', 
                  'TOTALDAMAGEDEALTTOCHAMPIONS']
df_detalles = df[columnas_stats]
df_detalles.to_csv("mysql_3_detalles.csv", index=False, encoding='utf-8')
print(f"  ✓ Creado: mysql_3_detalles.csv ({len(df_detalles)} registros)")


# --- 3. NORMALIZACIÓN PARA MONGODB ---
print("\n⚙️ Generando documento estructurado para MongoDB (Modo Rápido)...")
inicio_json = time.time()

# 1. Convertimos todo a diccionario de golpe
registros = df.to_dict(orient='records')

# 2. Usamos diccionarios nativos de Python para agrupar (vuela en comparación a Pandas)
partidas_dict = {}

for fila in registros:
    match_id = fila['P_MATCH_ID']
    
    # Si la partida no existe aún en nuestro diccionario, la creamos
    if match_id not in partidas_dict:
        partidas_dict[match_id] = {
            "MatchID": match_id,
            "GameVersion": fila['GAMEVERSION'],
            "Jugadores": []
        }
    
    # Extraemos solo los datos del jugador (quitando ID y Versión)
    datos_jugador = {clave: valor for clave, valor in fila.items() if clave not in ['P_MATCH_ID', 'GAMEVERSION']}
    
    # Metemos al jugador en la lista de su partida
    partidas_dict[match_id]["Jugadores"].append(datos_jugador)

# 3. Pasamos el diccionario a una lista final y guardamos
mongo_docs = list(partidas_dict.values())

with open("mongo_optimizado.json", "w", encoding="utf-8") as f:
    json.dump(mongo_docs, f, indent=4)

fin_json = time.time()
print(f"  ✓ Creado: mongo_optimizado.json ({len(mongo_docs)} partidas únicas)")
print(f"  ⏱️ Tiempo de generación JSON: {fin_json - inicio_json:.2f} segundos")
print("\n🚀 ¡Proceso de normalización completado con éxito!")