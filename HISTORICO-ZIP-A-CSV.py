import os
import zipfile
import csv
import io
from google.cloud import storage
from flask import Flask, request

app = Flask(__name__)

@app.route('/process_zip_files', methods=['POST'])
def process_zip_files(request):
    print("Iniciando...")

    # Nombre del bucket desde la variable de entorno
    bucket_name = 'bucket-prybd'
    
    if not bucket_name:
        return "La variable de entorno BUCKET_NAME no está configurada.", 400
    
    # Crear cliente de storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Listar los blobs en el bucket
    blobs = bucket.list_blobs()

    # Diccionario para almacenar el contenido combinado de los archivos txt por nombre
    combined_data = {}

    # Procesar cada archivo zip en el bucket
    n = 0
    for blob in blobs:
        n += 1
        
        if blob.name.endswith('.zip'):
            print(f"Accediendo al Zip {n}...")
            # Descargar el contenido del zip
            zip_contents = blob.download_as_bytes()
            zip_file = zipfile.ZipFile(io.BytesIO(zip_contents))

            # Procesar cada archivo txt dentro del zip
            for file_info in zip_file.infolist():
                if file_info.filename.endswith('.txt'):
                    with zip_file.open(file_info) as txt_file:
                        content = txt_file.read().decode('utf-8')
                        base_name = os.path.basename(file_info.filename)
                        
                        if base_name not in combined_data:
                            combined_data[base_name] = {
                                'header': None,
                                'rows': []
                            }

                        lines = content.splitlines()
                        header = lines[0].split(',')

                        # Si el encabezado no se ha guardado aún, guardarlo
                        if combined_data[base_name]['header'] is None:
                            combined_data[base_name]['header'] = header

                        # Añadir filas, omitiendo el encabezado
                        combined_data[base_name]['rows'].extend(lines[1:])

    # Guardar los archivos CSV combinados en GCS
    for filename, data in combined_data.items():
        if not data['rows']:
            continue

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        
        # Escribir el encabezado
        writer.writerow(data['header'])

        # Escribir las filas
        for line in data['rows']:
            writer.writerow(line.split(','))

        # Guardar el contenido CSV en GCS
        blob = bucket.blob(f'procesado/{filename.replace(".txt", ".csv")}')
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f'{filename} subido a {blob.public_url}')
    
    print("Funcion Terminada")
    return "Archivos procesados y combinados correctamente.", 200

if __name__ == '__main__':
    app.run(debug=True)

process_zip_files(request)
print("Proceso Terminado.")
