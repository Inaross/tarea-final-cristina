import csv
import mysql.connector
import time

# --- Configuracion (segun docker-compose.yaml) ---
HOST     = '127.0.0.1'
PORT     = 3306
DATABASE = 'test_db'
USER     = 'devuser'
PASSWORD = 'devpassword'
CHUNK    = 5000  # filas por INSERT

# --- Lista de archivos a subir y sus tablas destino ---
# Ajusta las rutas si no están en la misma carpeta que este script
TAREAS_IMPORTACION = [
    {"ruta": "D:/Big Data Aplicado/Optimizacion/Codigo/matchups128mb.csv", "tabla": "matchups_raw"},
    {"ruta": "mysql_1_invocadores.csv", "tabla": "norm_invocadores"},
    {"ruta": "mysql_2_partidas.csv", "tabla": "norm_partidas"},
    {"ruta": "mysql_3_detalles.csv", "tabla": "norm_detalles"}
]

# --- Conexion ---
print('Conectando a MySQL...')
con = mysql.connector.connect(host=HOST, port=PORT, database=DATABASE, user=USER, password=PASSWORD)
cur = con.cursor()

# --- Bucle de importación para cada archivo ---
for tarea in TAREAS_IMPORTACION:
    CSV_PATH = tarea["ruta"]
    TABLA = tarea["tabla"]
    
    print(f"\nProcesando archivo: {CSV_PATH} -> Tabla: {TABLA}")
    
    # 1. Leer cabecera
    try:
        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            columnas = next(reader)
    except FileNotFoundError:
        print(f"  ERROR: No se encontró el archivo {CSV_PATH}. Saltando...")
        continue

    # Limpiar columnas
    col_limpias = [c.strip() if c.strip() else f'col_{i+1}' for i, c in enumerate(columnas)]
    
    # 2. Crear tabla
    col_defs = ',\n  '.join([f'`{col}` TEXT' for col in col_limpias])
    cur.execute(f"DROP TABLE IF EXISTS `{TABLA}`")
    cur.execute(f"""
        CREATE TABLE `{TABLA}` (
          id INT AUTO_INCREMENT PRIMARY KEY,
          {col_defs}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    con.commit()
    print(f'  Tabla `{TABLA}` creada.')

    # 3. Insertar datos
    placeholders = ', '.join(['%s'] * len(col_limpias))
    col_names    = ', '.join([f'`{c}`' for c in col_limpias])
    sql_insert   = f"INSERT INTO `{TABLA}` ({col_names}) VALUES ({placeholders})"

    t0 = time.time()
    total = 0
    lote = []

    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # saltar cabecera
        for fila in reader:
            lote.append(fila)
            if len(lote) >= CHUNK:
                cur.executemany(sql_insert, lote)
                con.commit()
                total += len(lote)
                lote = []
                print(f'    Insertadas {total:,} filas...', end='\r')

        if lote:
            cur.executemany(sql_insert, lote)
            con.commit()
            total += len(lote)

    t1 = time.time()
    print(f'\n  Completado: {total:,} filas en {t1 - t0:.2f}s')

cur.close()
con.close()
print("\n¡Toda la importación de MySQL ha finalizado!")