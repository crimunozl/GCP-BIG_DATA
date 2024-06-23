import requests
import csv
import os
from google.cloud import storage
import json
import time

def fetch_services():
    print("Obteniendo servicios...")
    service_url = "https://www.red.cl/restservice_v2/rest/getservicios/all"
    response = requests.get(service_url)
    if response.status_code == 200:
        print("Servicios obtenidos con éxito")
        return response.json()
    else:
        print(f"Error al obtener servicios, código de estado: {response.status_code}")
        return []

def fetch_route(cod_sint):
    print(f"Obteniendo recorrido para {cod_sint}...")
    route_url = f"https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={cod_sint}"
    response = requests.get(route_url)
    if response.status_code == 200:
        print(f"Recorrido para {cod_sint} obtenido con éxito")
        return response.json()
    else:
        print(f"Error al obtener recorrido para {cod_sint}, código de estado: {response.status_code}")
        return {}

def upload_to_gcs(data, bucket_name, destination_blob_name):
    print("Subiendo datos a GCS...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    temp_file_path = '/tmp/route_data.csv'
    with open(temp_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "cod_sint", "negocio_id", "negocio_nombre", "negocio_color", "negocio_url",
            "ida_id", "ida_destino", "ida_itinerario", "ida_horarios", "ida_path", "ida_paraderos",
            "regreso_id", "regreso_destino", "regreso_itinerario", "regreso_horarios", "regreso_path", "regreso_paraderos"
        ])
        
        for entry in data:
            route_info = entry["info_recorrido"]
            writer.writerow([
                entry["cod_sint"],
                route_info.get("negocio", {}).get("id"), 
                route_info.get("negocio", {}).get("nombre"), 
                route_info.get("negocio", {}).get("color"), 
                route_info.get("negocio", {}).get("url"),
                route_info.get("ida", {}).get("id"), 
                route_info.get("ida", {}).get("destino"), 
                route_info.get("ida", {}).get("itinerario"),
                json.dumps(route_info.get("ida", {}).get("horarios")), 
                json.dumps(route_info.get("ida", {}).get("path")), 
                json.dumps(route_info.get("ida", {}).get("paraderos")),
                route_info.get("regreso", {}).get("id"), 
                route_info.get("regreso", {}).get("destino"), 
                route_info.get("regreso", {}).get("itinerario"),
                json.dumps(route_info.get("regreso", {}).get("horarios")), 
                json.dumps(route_info.get("regreso", {}).get("path")), 
                json.dumps(route_info.get("regreso", {}).get("paraderos"))
            ])

    blob.upload_from_filename(temp_file_path)
    print("Datos subidos a GCS con éxito")

def handle_request(event, context):
    print("Iniciando función handle_request...")
    bucket_name = "bucket_diario_json"
    services = fetch_services()
    collected_data = []
    batch_limit = 50  # Tamaño del lote
    delay_between_batches = 1  # Retraso en segundos entre lotes

    for index, service in enumerate(services):
        route_info = fetch_route(service)
        collected_data.append({"cod_sint": service, "info_recorrido": route_info})
        print(f"Recorrido para {service} añadido a collected_data")
        
        # Procesar en lotes
        if (index + 1) % batch_limit == 0:
            print(f"Procesando lote {index // batch_limit + 1}")
            upload_to_gcs(collected_data, bucket_name, f'conjunto_data_diaria_batch_{index // batch_limit + 1}.csv')
            collected_data = []  # Reiniciar collected_data para el siguiente lote
            time.sleep(delay_between_batches)  # Pausa para evitar sobrecarga

    # Guardar cualquier dato restante
    if collected_data:
        print("Procesando lote final")
        upload_to_gcs(collected_data, bucket_name, f'conjunto_data_diaria_batch_{(index // batch_limit) + 1}.csv')

    print("Función handle_request completada")
    return "Datos obtenidos y guardados en GCS", 200
