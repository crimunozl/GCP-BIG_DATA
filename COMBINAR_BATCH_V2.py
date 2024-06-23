import pandas as pd
from google.cloud import storage
import os
import base64
import json
from flask import escape

def merge_csv_files(event, context=None):
    try:
        # Decodificar el mensaje de Pub/Sub
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_json = json.loads(pubsub_message)
    except KeyError as e:
        print(f"KeyError: {e}. Asegúrate de que el mensaje contenga la clave 'data'.")
        return "Mensaje inválido: falta 'data'", 400
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e}. Asegúrate de que el mensaje esté en formato JSON.")
        return "Mensaje inválido: formato JSON incorrecto", 400
    
    # Asegúrate de que el mensaje contenga las claves necesarias
    if 'bucket' not in message_json or 'csv_files' not in message_json:
        print("Mensaje inválido: falta 'bucket' o 'csv_files'.")
        return "Mensaje inválido: falta 'bucket' o 'csv_files'", 400
    
    bucket_name = message_json['bucket']
    csv_files = message_json['csv_files']

    cloud_storage_client = storage.Client()
    bucket = cloud_storage_client.bucket(bucket_name)

    data_frames = []

    for csv_file in csv_files:
        print(f"Procesando archivo CSV: {csv_file}")
        blob = bucket.blob(csv_file)
        _, temp_local_filename = os.path.split(csv_file)
        blob.download_to_filename(f"/tmp/{temp_local_filename}")

        df = pd.read_csv(f"/tmp/{temp_local_filename}")
        data_frames.append(df)
        os.remove(f"/tmp/{temp_local_filename}")

    combined_df = pd.concat(data_frames, ignore_index=True)
    combined_csv_path = '/tmp/combined_data.csv'

    # Verificar si el archivo combinado ya existe en el bucket
    combined_blob = bucket.blob('combined/combined_data.csv')
    if not combined_blob.exists():
        print("El archivo combinado no existe. Creando y subiendo...")
        combined_df.to_csv(combined_csv_path, index=False)

        # Subir el archivo combinado de vuelta al bucket
        combined_blob.upload_from_filename(combined_csv_path)
        os.remove(combined_csv_path)
        print(f"Archivo CSV combinado subido a {bucket_name}/combined/combined_data.csv")
    else:
        print("El archivo combinado ya existe en el bucket. Sobrescribiendo...")

        # Eliminar el archivo combinado existente
        combined_blob.delete()

        # Guardar el DataFrame combinado como un archivo CSV local
        combined_df.to_csv(combined_csv_path, index=False)

        # Subir el nuevo archivo combinado al bucket
        combined_blob.upload_from_filename(combined_csv_path)
        os.remove(combined_csv_path)
        print(f"Archivo CSV combinado sobrescrito en {bucket_name}/combined/combined_data.csv")

    return "Proceso completado exitosamente.", 200
