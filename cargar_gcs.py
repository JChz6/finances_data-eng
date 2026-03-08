import sys
import os
import argparse
import pandas as pd
from google.cloud import storage

def upload_to_gcs(local_file_path, bucket_name="finanzas_personales_raw"):
    # 1. Preparar nombres y rutas
    base_name = os.path.basename(local_file_path)
    file_name_no_ext = os.path.splitext(base_name)[0]
    
    # Rutas dentro del bucket (prefijos/carpetas)
    gcs_xlsx_path = f"xlsx/{base_name}"
    gcs_csv_path = f"csv/{file_name_no_ext}.csv"
    
    # Ruta temporal local para el CSV
    temp_csv_path = f"{file_name_no_ext}_temp.csv"

    # 2. Configuración de GCS
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    CREDS_PATH = os.path.join(SCRIPT_DIR, "creds_trabajador_gcs.json")
    
    client = storage.Client.from_service_account_json(CREDS_PATH)
    bucket = client.bucket(bucket_name)

    try:
        # --- PASO A: Subir el XLSX original ---
        print(f"📤 Subiendo original: {base_name} -> gs://{bucket_name}/{gcs_xlsx_path}")
        blob_xlsx = bucket.blob(gcs_xlsx_path)
        blob_xlsx.upload_from_filename(local_file_path)

        # --- PASO B: Conversión local a CSV ---
        if local_file_path.endswith('.xlsx'):
            print(f"🔄 Convirtiendo a CSV sin alterar datos...")
            df = pd.read_excel(local_file_path)
            # index=False asegura que no se agreguen columnas adicionales
            df.to_csv(temp_csv_path, index=False, encoding='utf-8')

            # --- PASO C: Subir el CSV convertido ---
            print(f"📤 Subiendo versión CSV: {gcs_csv_path} -> gs://{bucket_name}/{gcs_csv_path}")
            blob_csv = bucket.blob(gcs_csv_path)
            blob_csv.upload_from_filename(temp_csv_path)
            
            print(f"✅ Proceso completado: Ambos archivos están en GCS.")
        else:
            print("⚠️ El archivo no es .xlsx, solo se subió el original.")

    except Exception as e:
        print(f"❌ Error durante el proceso: {e}")

    finally:
        # Limpieza del archivo temporal local
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            print(f"🗑️ Archivo temporal local eliminado.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sube XLSX y su versión CSV a carpetas separadas en GCS.")

    parser.add_argument("local_file_path", help="Ruta del archivo .xlsx local")
    parser.add_argument("--bucket", default="finanzas_personales_raw", help="Nombre del bucket de GCS")

    args = parser.parse_args()

    if os.path.exists(args.local_file_path):
        upload_to_gcs(args.local_file_path, args.bucket)
    else:
        print(f"❌ El archivo '{args.local_file_path}' no existe.")