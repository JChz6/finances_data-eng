import sys
import os
import argparse
import pandas as pd  # Necesaria para la conversión
from google.cloud import storage

def upload_to_gcs(local_file_path, bucket_name="finanzas_personales_raw", destination_blob_name=None):
    # 1. Verificar si es un archivo Excel para convertirlo
    temp_csv_path = None
    if local_file_path.endswith('.xlsx'):
        print(f"🔄 Convirtiendo '{local_file_path}' a CSV...")
        
        # Leemos el Excel
        # Nota: index=False es vital para no alterar los datos agregando columnas de índices
        df = pd.read_excel(local_file_path)
        
        # Definimos la ruta del archivo temporal .csv
        temp_csv_path = local_file_path.replace('.xlsx', '.csv')
        df.to_csv(temp_csv_path, index=False, encoding='utf-8')
        
        # Ajustamos las rutas para la subida
        upload_path = temp_csv_path
        if destination_blob_name:
            destination_blob_name = destination_blob_name.replace('.xlsx', '.csv')
        else:
            destination_blob_name = os.path.basename(temp_csv_path)
    else:
        upload_path = local_file_path
        if destination_blob_name is None:
            destination_blob_name = os.path.basename(local_file_path)

    # 2. Configuración de GCS
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    CREDS_PATH = os.path.join(SCRIPT_DIR, "creds_trabajador_gcs.json")

    try:
        client = storage.Client.from_service_account_json(CREDS_PATH)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Subida del archivo (ya sea el original o el CSV convertido)
        blob.upload_from_filename(upload_path)
        print(f"✅ Archivo '{destination_blob_name}' subido a ✅ gs://{bucket_name}/{destination_blob_name}")

    finally:
        # 3. Limpieza: Borramos el CSV temporal si fue creado para no dejar basura local
        if temp_csv_path and os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            print(f"🗑️ Archivo temporal '{temp_csv_path}' eliminado.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convierte XLSX a CSV y lo sube a Google Cloud Storage.")

    parser.add_argument("local_file_path", help="Ruta del archivo Excel local")
    parser.add_argument("--bucket", default="finanzas_personales_raw", help="Nombre del bucket de GCS")
    parser.add_argument("--dest", help="Nombre del archivo en el bucket (opcional)")

    args = parser.parse_args()

    upload_to_gcs(args.local_file_path, args.bucket, args.dest)