#Consultar un CSV como si fuera una base de datos SQL sin tener que "montar" un servidor MySQL completo 
#es uno de los trucos más útiles en el análisis de datos.

#En Windows 10, la forma más profesional y rápida de hacer esto es usando la librería DuckDB o el módulo sqlite3
# (que ya viene instalado con Python). 
#Aquí te enseño cómo hacerlo con DuckDB, que es el estándar actual por su velocidad "endiablada".
#Opción A: Usando DuckDB (La más rápida)

#DuckDB permite lanzar comandos SQL directamente sobre el archivo .csv como si fuera una tabla física, sin pasos intermedios.

import pandas as pd
import time

# --- 1. CONFIGURACIÓN ---
CSV_PATH = 'D:/Big Data Aplicado/Optimizacion/Codigo/matchups128mb.csv'

print(f"Cargando el dataset {CSV_PATH} en memoria RAM...")
inicio_carga = time.time()
df = pd.read_csv(CSV_PATH)
# Estandarizamos todas las columnas a mayúsculas para evitar fallos
df.columns = df.columns.str.upper()
print(f"Dataset cargado en {time.time() - inicio_carga:.2f} segundos. Total filas: {len(df):,}\n")


# --- 2. LAS 3 CONSULTAS OBLIGATORIAS (ROJAS) ---

def consulta_1_filtro_categoria(campeon_buscado):
    """(Roja 1) Filtrar por Categoría: Equivalente a WHERE CHAMPION = 'X'"""
    print(f"\nCONSULTA 1: Buscando partidas del campeón '{campeon_buscado}'...")
    inicio = time.time()
    
    # Filtro exacto usando la variable
    resultado = df[df['CHAMPION'].astype(str).str.lower() == campeon_buscado.lower()]
    
    tiempo_ms = (time.time() - inicio) * 1000
    print(f"Tiempo de ejecución: {tiempo_ms:.2f} ms")
    return resultado[['P_MATCH_ID', 'CHAMPION', 'WIN', 'KILLS', 'DEATHS']].head(3)


def consulta_2_carrileadas(min_kills):
    """Apps Freemium: Filtro múltiple, proyección y ordenación"""
    print(f"\nCONSULTA 2: 'Carrileadas' (Victorias con más de {min_kills} Kills, ordenadas por Oro)...")
    
    # Aseguramos formato numérico y texto
    df['KILLS'] = pd.to_numeric(df['KILLS'], errors='coerce')
    df['GOLDEARNED'] = pd.to_numeric(df['GOLDEARNED'], errors='coerce')
    df['WIN'] = df['WIN'].astype(str).str.lower()
    
    inicio = time.time()
    
    # Filtro múltiple usando la variable (min_kills)
    resultado = df[(df['WIN'] == 'true') & (df['KILLS'] > min_kills)]
    # Ordenación descendente
    resultado = resultado.sort_values(by='GOLDEARNED', ascending=False)
    
    tiempo_ms = (time.time() - inicio) * 1000
    print(f"Tiempo de ejecución: {tiempo_ms:.2f} ms")
    return resultado[['CHAMPION', 'WIN', 'KILLS', 'GOLDEARNED']].head(5)


def consulta_3_conteo_categorias():
    """Conteo Total: Agrupación Equivalente a GROUP BY CHAMPION"""
    print("\nCONSULTA 3: Conteo Total de Partidas por Campeón (Top 5)...")
    inicio = time.time()
    
    # Agrupación y conteo rápido
    resultado = df['CHAMPION'].value_counts()
    
    tiempo_ms = (time.time() - inicio) * 1000
    print(f"Tiempo de ejecución: {tiempo_ms:.2f} ms")
    return resultado.head(5)


# --- 3. EJECUCIÓN CON VARIABLES POR CONSOLA ---
if __name__ == "__main__":
    print("="*50)
    print(" SCRIPT DE CONSULTAS DIRECTAS AL CSV (PANDAS)")
    print("="*50)
    
    # Ejecutamos la Roja 1 pidiendo la variable al usuario
    var_campeon = input("1. Introduce un campeón para buscar (ej. Ahri, Garen, Blitzcrank): ")
    print(consulta_1_filtro_categoria(var_campeon))
    
    # Ejecutamos la Roja 2 pidiendo la variable al usuario
    var_kills = int(input("\n2. Introduce el mínimo de kills para considerar 'Carrileada' (ej. 10): "))
    print(consulta_2_carrileadas(var_kills))
    
    # Ejecutamos la Roja 3 (es general, no requiere variable)
    print(consulta_3_conteo_categorias())