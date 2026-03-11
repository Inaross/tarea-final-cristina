#Consultar un CSV como si fuera una base de datos SQL sin tener que "montar" un servidor MySQL completo 
#es uno de los trucos más útiles en el análisis de datos.

#En Windows 10, la forma más profesional y rápida de hacer esto es usando la librería DuckDB o el módulo sqlite3
# (que ya viene instalado con Python). 
#Aquí te enseño cómo hacerlo con DuckDB, que es el estándar actual por su velocidad "endiablada".
#Opción A: Usando DuckDB (La más rápida)

#DuckDB permite lanzar comandos SQL directamente sobre el archivo .csv como si fuera una tabla física, sin pasos intermedios.

import duckdb

# ruta al archivo
csv_path = 'D:/Big Data Aplicado/Optimizacion/archive/matchups.csv'

# parametros de busqueda
invocador = 'batata 12121212'
campeon = 'Bard'
min_kills = 15

print("Test de consultas directas a CSV con duckdb\n")

# buscar invocador
print(f"> Buscando invocador: {invocador}")
t0 = time.time()
df_invocador = duckdb.query(f"SELECT * FROM '{csv_path}' WHERE SUMMONERNAME = '{invocador}'").df()
t1 = time.time()
print(df_invocador.head(2))
print(f"Tiempo: {t1 - t0:.4f}s\n")

# filtrar por campeon
print(f"> Filtrando por campeon: {campeon}")
t0 = time.time()
df_campeon = duckdb.query(f"SELECT * FROM '{csv_path}' WHERE CHAMPION = '{campeon}'").df()
t1 = time.time()
print(f"Partidas encontradas: {len(df_campeon)}")
print(f"Tiempo: {t1 - t0:.4f}s\n")

# buscar victorias con x kills ordenado natural (mas oro)
print(f"> Top 5 victorias con >{min_kills} kills ordenadas por oro")
t0 = time.time()
q_compleja = f"""
    SELECT SUMMONERNAME, CHAMPION, GOLDEARNED 
    FROM '{csv_path}' 
    WHERE WIN = True AND KILLS > {min_kills} 
    ORDER BY GOLDEARNED DESC 
    LIMIT 5
"""
df_kills = duckdb.query(q_compleja).df()
t1 = time.time()
print(df_kills)
print(f"Tiempo: {t1 - t0:.4f}s\n")

# total de partidas por campeon
print("> Partidas totales por campeon (top 5)")
t0 = time.time()
q_agrupada = f"""
    SELECT CHAMPION, COUNT(*) as total 
    FROM '{csv_path}' 
    GROUP BY CHAMPION 
    ORDER BY total DESC 
    LIMIT 5
"""
df_agrupado = duckdb.query(q_agrupada).df()
t1 = time.time()
print(df_agrupado)
print(f"Tiempo: {t1 - t0:.4f}s\n")