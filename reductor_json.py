import json
import os

def reducir_json_por_tamano(ruta_origen, ruta_destino, tamano_max_mb=128):
    # Aplicar un margen de seguridad del 5% para no chocar con el límite exacto de subida HTTP (128M)
    tamano_max_bytes = int(tamano_max_mb * 1024 * 1024 * 0.95)
    
    # El tamaño base de un array JSON vacío es de 2 bytes: "[]"
    tamano_actual = 2 
    elementos_guardados = []
    
    try:
        print("1. Cargando el archivo JSON original...")
        with open(ruta_origen, 'r', encoding='utf-8') as archivo:
            datos = json.load(archivo)
            
        # Verificamos que sea un conjunto de datos válido (un Array)
        if not isinstance(datos, list):
            print("Error: El archivo JSON debe contener una lista (array) de objetos en su raíz.")
            return

        print("2. Calculando el peso y extrayendo elementos...")
        for elemento in datos:
            # Convertimos el elemento a texto (minificado) para medir sus bytes reales
            elemento_str = json.dumps(elemento, ensure_ascii=False, separators=(',', ':'))
            bytes_elemento = len(elemento_str.encode('utf-8'))
            
            # Sumamos 1 byte extra por la coma separadora de JSON (excepto en el primer elemento)
            bytes_adicionales = bytes_elemento if len(elementos_guardados) == 0 else bytes_elemento + 1

            # 3. Comprobar el límite de bytes
            if tamano_actual + bytes_adicionales > tamano_max_bytes:
                break
                
            elementos_guardados.append(elemento)
            tamano_actual += bytes_adicionales

        # 4. Guardar el nuevo archivo garantizando una estructura perfecta
        print("3. Generando el nuevo archivo estructurado...")
        with open(ruta_destino, 'w', encoding='utf-8') as archivo_salida:
            # Usamos separators para no añadir bytes extra en espacios o saltos de línea
            json.dump(elementos_guardados, archivo_salida, ensure_ascii=False, separators=(',', ':'))

        print(f"\n¡Éxito! El archivo JSON se ha reducido manteniendo su validez.")
        print(f"-> Archivo generado: {ruta_destino}")
        print(f"-> Tamaño final aproximado: {tamano_actual / (1024 * 1024):.2f} MB")
        print(f"-> Total de objetos copiados: {len(elementos_guardados)}")

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo en la ruta '{ruta_origen}'.")
    except json.JSONDecodeError:
        print("Error: El archivo original no tiene un formato JSON válido o está corrupto.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

# --- Ejecución del script ---
if __name__ == "__main__":
    # Define aquí las rutas de tus archivos JSON
    archivo_entrada = 'D:\\Big Data Aplicado\\Optimizacion\\archive\\matchups.json'
    archivo_salida =  'D:\\Big Data Aplicado\\Optimizacion\\Codigo\\matchups128mb.json'
    
    reducir_json_por_tamano(archivo_entrada, archivo_salida, tamano_max_mb=128)