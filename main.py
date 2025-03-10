import os
import pandas as pd
from datetime import datetime
import pytz
from google.cloud import bigquery, storage
import functions_framework
import logging


utc_minus_5 = pytz.timezone('America/Lima')

# Configurar el logging
logging.basicConfig(level=logging.INFO)

'''
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
'''

def presup_to_bigquery_temp(df, table_id, temp_table_id):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema = [
            bigquery.SchemaField("fecha", "DATE"),
            bigquery.SchemaField("year", "STRING"),
            bigquery.SchemaField("month", "STRING"),
            bigquery.SchemaField("categoria", "STRING"),
            bigquery.SchemaField("presupuesto", "FLOAT64")
        ],
        source_format = bigquery.SourceFormat.PARQUET,
    )

    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()
    logging.info(f"Cargado el presupuesto en la temporal {temp_table_id}")

    merge_query = f'''
    MERGE `{table_id}` AS target
    USING `{temp_table_id}` AS source
    ON target.fecha = source.fecha AND target.categoria = source.categoria
    WHEN MATCHED THEN
        UPDATE SET
            target.presupuesto = source.presupuesto,
            target.year = source.year,
            target.month = source.month
    WHEN NOT MATCHED THEN
        INSERT (fecha, year, month, categoria, presupuesto)
        VALUES (source.fecha, source.year, source.month, source.categoria, source.presupuesto);
    '''
    query_job = client.query(merge_query)
    query_job.result()
    logging.info(f"MERGE de presupuesto en {table_id}")

    # Eliminar la tabla temporal
    client.delete_table(temp_table_id, not_found_ok=True)
    logging.info(f"ELIMINADA temporal: {temp_table_id}")

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
            bigquery.SchemaField("moneda", "STRING"),
            bigquery.SchemaField("comentario", "STRING"),
            bigquery.SchemaField("fecha_carga", "DATETIME"),
            bigquery.SchemaField("dias_trabajados", "FLOAT64")
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
            df.drop(columns=['Importe', 'Cuentas.1'], inplace=True)
            df['Según un período'] = pd.to_datetime(df['Según un período']).dt.date
            df.rename(columns={
                'Según un período': 'fecha',
                'Cuentas': 'cuenta',
                'Categoría': 'categoria',
                'Subcategorías': 'subcategoria',
                'Nota': 'nota',
                'Ingreso/Gasto': 'ingreso_gasto',
                'Descripción':'comentario',
                'PEN': 'importe',
                'Moneda': 'moneda'
            }, inplace=True)
            df['categoria'] = df['categoria'].str.strip()
            df['subcategoria'] = df['subcategoria'].str.strip()
            df['nota'] = df['nota'].str.strip()
            df['ingreso_gasto'] = df['ingreso_gasto'].str.strip()
            df['comentario'] = df['comentario'].str.strip()
            df['fecha_carga'] = datetime.now(pytz.utc).astimezone(utc_minus_5)
            df['fecha_carga'] = df['fecha_carga'].dt.tz_localize(None)
            
            
            # Convertir la columna 'comentario' a string, normalizar y buscar la cadena
            mask = (
                df['comentario']
                    .astype(str)
                    .str.replace('í', 'i')
                    .str.lower()
                    .str.contains('dias trabajados')
                )

            df.loc[mask, 'dias_trabajados'] = (
                df.loc[mask, 'comentario']
                .str.replace('í', 'i')  # Eliminar acento en 'í'
                .str.lower()  # Convertir a minúsculas
                .str.replace('dias trabajados', '', case=False, regex=False)
                .str.strip()
                .astype(float)
                )
            
            #df['dias_trabajados'] = pd.to_numeric(df['dias_trabajados'], errors='coerce')

            logging.info("Datos transformados correctamente para el archivo .xlsx")
            upload_to_bigquery(df, 'big-query-406221.finanzas_personales.historico')

        elif file_name.endswith('.csv'):
            df = pd.read_csv(temp_file_path, delimiter = ';', encoding='latin1')
            # Realizar las transformaciones necesarias y cargar a BigQuery
            df = df.dropna(subset=['fecha'])
            df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m-%d')
            df['fecha'] = pd.to_datetime(df['fecha'])
            df['year'] = df['year'].astype(int).astype(str)
            df['month'] = df['month'].astype(int).astype(str) 
            df = df[['fecha', 'year', 'month', 'categoria', 'presupuesto']]
            df = df.dropna(subset=['fecha'])
            logging.info("Datos transformados correctamente para el archivo .csv")
            
            temp_table_id = "big-query-406221.finanzas_personales.temp_presupuesto"
            presup_to_bigquery_temp(df, 'big-query-406221.finanzas_personales.presupuesto', temp_table_id)



    except Exception as e:
        logging.error(f"Error procesando el archivo {file_name}: {str(e)}")

    finally:
        # Eliminar el archivo local después de la carga
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        logging.info(f"Archivo temporal {temp_file_path} eliminado")