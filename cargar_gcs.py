#!pip install google-cloud-storage

import os
import argparse
from google.cloud import storage

def upload_to_gcs(local_file_path, bucket_name="finanzas_personales_raw", destination_blob_name=None):

    if destination_blob_name is None:
        destination_blob_name = os.path.basename(local_file_path)  # Usa el mismo nombre del archivo local
    
    client = storage.Client.from_service_account_json("creds_trabajador_gcs.json")
    
    bucket = client.bucket(bucket_name)
    
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_filename(local_file_path)

    print(f"✅ Archivo '{local_file_path}' subido a ✅ gs://{bucket_name}/{destination_blob_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sube un archivo a Google Cloud Storage.")

    parser.add_argument("local_file_path", help="Ruta del archivo local a subir")
    parser.add_argument("--bucket", default="finanzas_personales_raw", help="Nombre del bucket de GCS")
    parser.add_argument("--dest", help="Nombre del archivo en el bucket (opcional, por defecto usa el nombre original)")

    args = parser.parse_args()

    upload_to_gcs(args.local_file_path, args.bucket, args.dest)
