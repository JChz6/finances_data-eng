import os
import pandas as pd
from google.cloud import bigquery, storage
import functions_framework
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO)

# Cargar los presupuestos
def presup_to_bigquery(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("fecha", "DATE"),
            bigquery.SchemaField("year", "STRING"),
            bigquery.SchemaField("month", "STRING"),
            bigquery.SchemaField("categoria", "STRING"),
            bigquery.SchemaField("presupuesto", "FLOAT64")
        ],
        source_format=bigquery.SourceFormat.PARQUET,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logging.info(f"Cargado {job.output_rows} filas en {table_id}")


#Cargar los registros
def upload_to_bigquery(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("fecha", "DATE"),
            bigquery.SchemaField("cuenta", "STRING"),
            bigquery.SchemaField("categoria", "STRING"),
            bigquery.SchemaField("subcategoria", "STRING"),
            bigquery.SchemaField("nota", "STRING"),
            bigquery.SchemaField("ingreso_gasto", "STRING"),
            bigquery.SchemaField("importe", "FLOAT64"),
            bigquery.SchemaField("moneda", "STRING")
        ],
        source_format=bigquery.SourceFormat.PARQUET,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logging.info(f"Cargado {job.output_rows} filas en {table_id}")

#Funcion de entrada
@functions_framework.cloud_event
def handle_gcs_event(cloud_event):
    data = cloud_event.data
    bucket_name = data['bucket']
    file_name = data['name']

    logging.info(f"Evento recibido para el archivo {file_name} en el bucket {bucket_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    temp_file_path = f"/tmp/{file_name}"

    try:
        logging.info(f"Descargando archivo {file_name} desde el bucket {bucket_name}")
        blob.download_to_filename(temp_file_path)
        logging.info(f"Archivo descargado a {temp_file_path}")

        if file_name.endswith('.xlsx'):
            df = pd.read_excel(temp_file_path, sheet_name='Sheet1')
            df.drop(columns=['Importe', 'Descripción', 'Cuentas.1'], inplace=True)
            df['Según un período'] = pd.to_datetime(df['Según un período']).dt.date
            df.rename(columns={
                'Según un período': 'fecha',
                'Cuentas': 'cuenta',
                'Categoría': 'categoria',
                'Subcategorías': 'subcategoria',
                'Nota': 'nota',
                'Ingreso/Gasto': 'ingreso_gasto',
                'PEN': 'importe',
                'Moneda': 'moneda'
            }, inplace=True)
            logging.info("Datos transformados correctamente para el archivo .xlsx")
            upload_to_bigquery(df, 'big-query-406221.finanzas_personales.historico')

        elif file_name.endswith('.csv'):
            df = pd.read_csv(temp_file_path, delimiter = ';', encoding='latin1')
            # Realizar las transformaciones necesarias y cargar a BigQuery
            df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m-%d')
            df['fecha'] = pd.to_datetime(df['fecha'])
            df['year'] = round(df['year'], 0)
            df['month'] = round(df['month'], 0)
            df['year'] = df['year'].astype(str)
            df['month'] = df['month'].astype(str) 
            df = df[['fecha', 'year', 'month', 'categoria', 'presupuesto']]
            logging.info("Datos transformados correctamente para el archivo .csv")
            presup_to_bigquery(df, 'big-query-406221.finanzas_personales.presupuesto')



    except Exception as e:
        logging.error(f"Error procesando el archivo {file_name}: {str(e)}")

    finally:
        # Eliminar el archivo local después de la carga
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        logging.info(f"Archivo temporal {temp_file_path} eliminado")