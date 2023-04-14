import json
from google.cloud import bigquery
from google.cloud import storage

# pre-defined
PROJECT_ID = 'ca-miyazaki-test'
# required
DATASET_NAME = 'etl_training_dataset'
# optiona;
DESTINATION_TABLE_NAME = 'raw_data'

def main(data, context):
    """GCSのバケットで更新があった場合に実行される関数
    """
    # BigQueryのClientインスタンスを作成
    bq_client = bigquery.client.Client(project=PROJECT_ID)

    # Functionsの引数（google.storage.object.finalizeオブジェクト）からバケット名、ファイル名を取得
    bucket_name = data['bucket']
    file_name = data['name']

    # loadする関数を実行
    job_id_load = gcs_to_bq(bucket_name, file_name, bq_client)

    print('job_id_load : {}'.format(job_id_load))

def gcs_to_bq(bucket_name, file_name, bq_client):
    """GCSバケットのCSVファイルをBigQueryにloadする関数
    """

    source_uri = 'gs://{}/{}'.format(bucket_name, file_name)
    print('import from {} to {}'.format(source_uri, DESTINATION_TABLE_NAME))
    dataset_ref = bigquery.dataset.DatasetReference(PROJECT_ID, DATASET_NAME)
    job_config = bigquery.job.LoadJobConfig()
    # schema.jsonが保存されているGCSのバケットとオブジェクトを指定
    schema_bucket_name = 'miyazaki-tutorial'
    schema_object_name = 'schema.json'
    # GCSのバケットとオブジェクトを使用して、スキーマ定義JSONファイルを読み込む
    storage_client = storage.Client()
    schema_bucket = storage_client.bucket(schema_bucket_name)
    blob = schema_bucket.blob(schema_object_name)
    schema_json = blob.download_as_string()
    # schemaの設定
    job_config.schema = [bigquery.schema.SchemaField(field['name'], field['type']) for field in json.loads(schema_json)]
    # CSVファイルをスキップする行数(default: 0)
    job_config.skip_leading_rows = 1
    # ファイルフォーマットはCSV(default: CSV)
    job_config.source_format = bigquery.SourceFormat.CSV
    # テーブルを上書き更新(default: WRITE_APPEND)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # loadジョブを実行開始
    load_job = bq_client.load_table_from_uri(source_uri, dataset_ref.table(DESTINATION_TABLE_NAME), job_config=job_config)
    print('Starting job {}'.format(load_job.job_id))

    # loadジョブの完了を待つ
    result = load_job.result()
    print('Job finished')


    # loadしたテーブルの行数を取得して出力
    print('Loaded {} rows.'.format(result.output_rows))

    return load_job.job_id