import os

def reducir_csv_por_tamano(ruta_origen, ruta_destino, tamano_max_mb=128):
    # Aplicar un margen de seguridad del 5% para no chocar con el límite exacto de subida HTTP (128M)
    tamano_max_bytes = int(tamano_max_mb * 1024 * 1024 * 0.95)
    tamano_actual = 0

    try:
        with open(ruta_origen, 'r', encoding='utf-8') as archivo_origen, \
             open(ruta_destino, 'w', encoding='utf-8') as archivo_destino:

            # 1. Leer, escribir y medir el encabezado
            encabezado = archivo_origen.readline()
            if not encabezado:
                print("El archivo original está vacío.")
                return

            archivo_destino.write(encabezado)
            tamano_actual += len(encabezado.encode('utf-8'))

            # 2. Procesar el resto del archivo línea por línea
            filas_copiadas = 0
            for linea in archivo_origen:
                tamano_linea = len(linea.encode('utf-8'))

                # 3. Comprobar el límite antes de escribir
                if tamano_actual + tamano_linea > tamano_max_bytes:
                    break

                archivo_destino.write(linea)
                tamano_actual += tamano_linea
                filas_copiadas += 1

        print(f"¡Éxito! El archivo se ha reducido correctamente.")
        print(f"-> Archivo generado: {ruta_destino}")
        print(f"-> Tamaño final: {tamano_actual / (1024 * 1024):.2f} MB")
        print(f"-> Total de filas copiadas: {filas_copiadas}")

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo original en la ruta '{ruta_origen}'.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

# --- Ejecución del script ---
if __name__ == "__main__":
    # Define aquí las rutas de tus archivos
    archivo_entrada = 'D:\\Big Data Aplicado\\Optimizacion\\archive\\matchups.csv'
    archivo_salida = 'D:\\Big Data Aplicado\\Optimizacion\\Codigo\\matchups128mb.csv'
    
    reducir_csv_por_tamano(archivo_entrada, archivo_salida, tamano_max_mb=128)