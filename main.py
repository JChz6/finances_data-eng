import os
import pandas as pd
from datetime import datetime
import pytz
from google.cloud import bigquery, storage
import functions_framework
import logging
from google.cloud.bigquery import QueryJobConfig


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

#Elimina registros existentes del mes que va a ingresar
def delete_old_data_from_bigquery(year_months, table_id):
    client = bigquery.Client()

    where_conditions = [
            f"(EXTRACT(YEAR FROM fecha) = {year} AND EXTRACT(MONTH FROM fecha) = {month})"
            for year, month in year_months
        ]

    combined_where_clause = " OR ".join(where_conditions)

    query = f"""
    DELETE FROM `{table_id}`
    WHERE {combined_where_clause}
    """
    logging.info(f"Preparando para eliminar registros de {len(year_months)} periodo(s) en {table_id}")
    logging.info(f"Consulta a ejecutar: {query}") # Útil para depuración

    try:
        # Ejecutar la única consulta de BigQuery
        query_job = client.query(query, job_config=QueryJobConfig())
        query_job.result()  # Espera a que el trabajo de eliminación finalice
        logging.info(f"🗑️ Eliminados exitosamente registros de {len(year_months)} periodo(s) en {table_id}.")
    except Exception as e:
        logging.error(f"❌ Error al intentar eliminar registros de {table_id}: {e}")


#Cargar los registros en histórico
def upload_to_historico(df, table_id):
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
            bigquery.SchemaField("dias_trabajados", "FLOAT64"),
            bigquery.SchemaField("clave", "STRING"),
            bigquery.SchemaField("valor", "STRING")
        ],
        source_format=bigquery.SourceFormat.PARQUET,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logging.info(f"Cargado {job.output_rows} filas en {table_id}")


#Cargar los registros en emocional
def upload_to_table(df, table_id):
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
            bigquery.SchemaField("clave", "STRING"),
            bigquery.SchemaField("valor", "STRING")
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
            df.drop(columns=['PEN', 'Cuentas.1'], inplace=True)
            df['Según un período'] = pd.to_datetime(df['Según un período']).dt.date
            df.rename(columns={
                'Según un período': 'fecha',
                'Cuentas': 'cuenta',
                'Categoría': 'categoria',
                'Subcategorías': 'subcategoria',
                'Nota': 'nota',
                'Ingreso/Gasto': 'ingreso_gasto',
                'Descripción':'comentario',
                'Importe': 'importe',
                'Moneda': 'moneda'
            }, inplace=True)
            df['categoria'] = df['categoria'].str.strip()
            df['subcategoria'] = df['subcategoria'].str.strip()
            df['nota'] = df['nota'].str.strip()
            df['ingreso_gasto'] = df['ingreso_gasto'].str.strip()
            df['comentario'] = df['comentario'].str.strip()
            df['fecha_carga'] = datetime.now(pytz.utc).astimezone(utc_minus_5)
            df['fecha_carga'] = df['fecha_carga'].dt.tz_localize(None)
            
            #  Extraer año-mes únicos
            df["anio_mes"] = df["fecha"].apply(lambda x: (x.year, x.month))
            unique_year_months = df["anio_mes"].drop_duplicates().tolist()
            
            # Eliminar registros en BigQuery para esos año-mes
            table_id = "big-query-406221.finanzas_personales.historico"
            emocional = "big-query-406221.finanzas_personales.emocional"
            kilometraje = "big-query-406221.finanzas_personales.kilometraje"
            delete_old_data_from_bigquery(unique_year_months, table_id)
            delete_old_data_from_bigquery(unique_year_months, kilometraje)
            delete_old_data_from_bigquery(unique_year_months, emocional)

            # Convertir la columna 'comentario' a string, normalizar y buscar la cadena
            mask_dias_trabajados = (
                df['comentario']
                    .astype(str)
                    .str.replace('í', 'i')
                    .str.lower()
                    .str.contains('dias trabajados')
                )

            df.loc[mask_dias_trabajados, 'dias_trabajados'] = (
                df.loc[mask_dias_trabajados, 'comentario']
                .str.replace('í', 'i')  # Eliminar acento en 'í'
                .str.lower()  # Convertir a minúsculas
                .str.replace('dias trabajados', '', case=False, regex=False)
                .str.strip()
                .astype(float)
                )
            

            # Busca claves y valores en la columna "comentario"
            df[['clave', 'valor']] = df['comentario'].str.extract(r'^\s*([^\s/]+/)\s*(\S+)', expand=True)

            df_finanzas = df[~df['cuenta'].isin(['Personal', 'Kilometraje'])]
            df_emocional = df[df['cuenta'] == 'Personal']
            df_kilometraje = df[df['cuenta'] == 'Kilometraje']

            logging.info("✅ Datos transformados correctamente para el archivo .xlsx")
            upload_to_historico(df_finanzas.drop(columns=["anio_mes"]), 'big-query-406221.finanzas_personales.historico')
            upload_to_table(df_emocional.drop(columns=["anio_mes", "dias_trabajados"]), 'big-query-406221.finanzas_personales.emocional')
            upload_to_table(df_kilometraje.drop(columns=["anio_mes", "dias_trabajados"]), 'big-query-406221.finanzas_personales.kilometraje')

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
            #presup_to_bigquery_temp(df, 'big-query-406221.finanzas_personales.presupuesto', temp_table_id)



    except Exception as e:
        logging.error(f"Error procesando el archivo {file_name}: {str(e)}")

    finally:
        # Eliminar el archivo local después de la carga
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        logging.info(f"Archivo temporal {temp_file_path} eliminado")