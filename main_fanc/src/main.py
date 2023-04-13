from google.cloud import bigquery

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
    # schemaの設定
    job_config.schema = [
        bigquery.schema.SchemaField('gameId', 'STRING'),
        bigquery.schema.SchemaField('seasonId', 'STRING'),
        bigquery.schema.SchemaField('seasonType', 'STRING'),
        bigquery.schema.SchemaField('year', 'INTEGER'),
        bigquery.schema.SchemaField('startTime', 'TIMESTAMP'),
        bigquery.schema.SchemaField('gameStatus', 'STRING'),
        bigquery.schema.SchemaField('attendance', 'INTEGER'),
        bigquery.schema.SchemaField('dayNight', 'STRING'),
        bigquery.schema.SchemaField('duration', 'STRING'),
        bigquery.schema.SchemaField('durationMinutes', 'INTEGER'),
        bigquery.schema.SchemaField('awayTeamId', 'STRING'),
        bigquery.schema.SchemaField('awayTeamName', 'STRING'),
        bigquery.schema.SchemaField('homeTeamId', 'STRING'),
        bigquery.schema.SchemaField('homeTeamName', 'STRING'),
        bigquery.schema.SchemaField('venueId', 'STRING'),
        bigquery.schema.SchemaField('venueName', 'STRING'),
        bigquery.schema.SchemaField('venueSurface', 'STRING'),
        bigquery.schema.SchemaField('venueCapacity', 'INTEGER'),
        bigquery.schema.SchemaField('venueCity', 'STRING'),
        bigquery.schema.SchemaField('venueState', 'STRING'),
        bigquery.schema.SchemaField('venueZip', 'INTEGER'),
        bigquery.schema.SchemaField('venueMarket', 'STRING'),
        bigquery.schema.SchemaField('venueOutfieldDistances', 'STRING'),
        bigquery.schema.SchemaField('homeFinalRuns', 'INTEGER'),
        bigquery.schema.SchemaField('homeFinalHits', 'INTEGER'),
        bigquery.schema.SchemaField('homeFinalErrors', 'INTEGER'),
        bigquery.schema.SchemaField('awayFinalRuns', 'INTEGER'),
        bigquery.schema.SchemaField('awayFinalHits', 'INTEGER'),
        bigquery.schema.SchemaField('awayFinalErrors', 'INTEGER'),
        bigquery.schema.SchemaField('homeFinalRunsForInning', 'INTEGER'),
        bigquery.schema.SchemaField('awayFinalRunsForInning', 'INTEGER'),
        bigquery.schema.SchemaField('inningNumber', 'INTEGER'),
        bigquery.schema.SchemaField('inningHalf', 'STRING'),
        bigquery.schema.SchemaField('inningEventType', 'STRING'),
        bigquery.schema.SchemaField('inningHalfEventSequenceNumber', 'INTEGER'),
        bigquery.schema.SchemaField('description', 'STRING'),
        bigquery.schema.SchemaField('atBatEventType', 'STRING'),
        bigquery.schema.SchemaField('atBatEventSequenceNumber', 'INTEGER'),
        bigquery.schema.SchemaField('createdAt', 'TIMESTAMP'),
        bigquery.schema.SchemaField('updatedAt', 'TIMESTAMP'),
        bigquery.schema.SchemaField('status', 'STRING'),
        bigquery.schema.SchemaField('outcomeId', 'STRING'),
        bigquery.schema.SchemaField('outcomeDescription', 'STRING'),
        bigquery.schema.SchemaField('hitterId', 'STRING'),
        bigquery.schema.SchemaField('hitterLastName', 'STRING'),
        bigquery.schema.SchemaField('hitterFirstName', 'STRING'),
        bigquery.schema.SchemaField('hitterWeight', 'INTEGER'),
        bigquery.schema.SchemaField('hitterHeight', 'INTEGER'),
        bigquery.schema.SchemaField('hitterBatHand', 'STRING'),
        bigquery.schema.SchemaField('pitcherId', 'STRING'),
        bigquery.schema.SchemaField('pitcherFirstName', 'STRING'),
        bigquery.schema.SchemaField('pitcherLastName', 'STRING'),
        bigquery.schema.SchemaField('pitcherThrowHand', 'STRING'),
        bigquery.schema.SchemaField('pitchType', 'STRING'),
        bigquery.schema.SchemaField('pitchTypeDescription', 'STRING'),
        bigquery.schema.SchemaField('pitchSpeed', 'INTEGER'),
        bigquery.schema.SchemaField('pitchZone', 'INTEGER'),
        bigquery.schema.SchemaField('pitcherPitchCount', 'INTEGER'),
        bigquery.schema.SchemaField('hitterPitchCount', 'INTEGER'),
        bigquery.schema.SchemaField('hitLocation', 'INTEGER'),
        bigquery.schema.SchemaField('hitType', 'STRING'),
        bigquery.schema.SchemaField('startingBalls', 'INTEGER'),
        bigquery.schema.SchemaField('startingStrikes', 'INTEGER'),
        bigquery.schema.SchemaField('startingOuts', 'INTEGER'),
        bigquery.schema.SchemaField('balls', 'INTEGER'),
        bigquery.schema.SchemaField('strikes', 'INTEGER'),
        bigquery.schema.SchemaField('outs', 'INTEGER'),
        bigquery.schema.SchemaField('rob0_start', 'STRING'),
        bigquery.schema.SchemaField('rob0_end', 'INTEGER'),
        bigquery.schema.SchemaField('rob0_isOut', 'STRING'),
        bigquery.schema.SchemaField('rob0_outcomeId', 'STRING'),
        bigquery.schema.SchemaField('rob0_outcomeDescription', 'STRING'),
        bigquery.schema.SchemaField('rob1_start', 'STRING'),
        bigquery.schema.SchemaField('rob1_end', 'INTEGER'),
        bigquery.schema.SchemaField('rob1_isOut', 'STRING'),
        bigquery.schema.SchemaField('rob1_outcomeId', 'STRING'),
        bigquery.schema.SchemaField('rob1_outcomeDescription', 'STRING'),
        bigquery.schema.SchemaField('rob2_start', 'STRING'),
        bigquery.schema.SchemaField('rob2_end', 'INTEGER'),
        bigquery.schema.SchemaField('rob2_isOut', 'STRING'),
        bigquery.schema.SchemaField('rob2_outcomeId', 'STRING'),
        bigquery.schema.SchemaField('rob2_outcomeDescription', 'STRING'),
        bigquery.schema.SchemaField('rob3_start', 'STRING'),
        bigquery.schema.SchemaField('rob3_end', 'INTEGER'),
        bigquery.schema.SchemaField('rob3_isOut', 'STRING'),
        bigquery.schema.SchemaField('rob3_outcomeId', 'STRING'),
        bigquery.schema.SchemaField('rob3_outcomeDescription', 'STRING'),
        bigquery.schema.SchemaField('is_ab', 'INTEGER'),
        bigquery.schema.SchemaField('is_ab_over', 'INTEGER'),
        bigquery.schema.SchemaField('is_hit', 'INTEGER'),
        bigquery.schema.SchemaField('is_on_base', 'INTEGER'),
        bigquery.schema.SchemaField('is_bunt', 'INTEGER'),
        bigquery.schema.SchemaField('is_bunt_shown', 'INTEGER'),
        bigquery.schema.SchemaField('is_double_play', 'INTEGER'),
        bigquery.schema.SchemaField('is_triple_play', 'INTEGER'),
        bigquery.schema.SchemaField('is_wild_pitch', 'INTEGER'),
        bigquery.schema.SchemaField('is_passed_ball', 'INTEGER'),
        bigquery.schema.SchemaField('homeCurrentTotalRuns', 'INTEGER'),
        bigquery.schema.SchemaField('awayCurrentTotalRuns', 'INTEGER'),
        bigquery.schema.SchemaField('awayFielder1', 'STRING'),
        bigquery.schema.SchemaField('awayFielder2', 'STRING'),
        bigquery.schema.SchemaField('awayFielder3', 'STRING'),
        bigquery.schema.SchemaField('awayFielder4', 'STRING'),
        bigquery.schema.SchemaField('awayFielder5', 'STRING'),
        bigquery.schema.SchemaField('awayFielder6', 'STRING'),
        bigquery.schema.SchemaField('awayFielder7', 'STRING'),
        bigquery.schema.SchemaField('awayFielder8', 'STRING'),
        bigquery.schema.SchemaField('awayFielder9', 'STRING'),
        bigquery.schema.SchemaField('awayFielder10', 'STRING'),
        bigquery.schema.SchemaField('awayFielder11', 'STRING'),
        bigquery.schema.SchemaField('awayFielder12', 'STRING'),
        bigquery.schema.SchemaField('awayBatter1', 'STRING'),
        bigquery.schema.SchemaField('awayBatter2', 'STRING'),
        bigquery.schema.SchemaField('awayBatter3', 'STRING'),
        bigquery.schema.SchemaField('awayBatter4', 'STRING'),
        bigquery.schema.SchemaField('awayBatter5', 'STRING'),
        bigquery.schema.SchemaField('awayBatter6', 'STRING'),
        bigquery.schema.SchemaField('awayBatter7', 'STRING'),
        bigquery.schema.SchemaField('awayBatter8', 'STRING'),
        bigquery.schema.SchemaField('awayBatter9', 'STRING'),
        bigquery.schema.SchemaField('homeFielder1', 'STRING'),
        bigquery.schema.SchemaField('homeFielder2', 'STRING'),
        bigquery.schema.SchemaField('homeFielder3', 'STRING'),
        bigquery.schema.SchemaField('homeFielder4', 'STRING'),
        bigquery.schema.SchemaField('homeFielder5', 'STRING'),
        bigquery.schema.SchemaField('homeFielder6', 'STRING'),
        bigquery.schema.SchemaField('homeFielder7', 'STRING'),
        bigquery.schema.SchemaField('homeFielder8', 'STRING'),
        bigquery.schema.SchemaField('homeFielder9', 'STRING'),
        bigquery.schema.SchemaField('homeFielder10', 'STRING'),
        bigquery.schema.SchemaField('homeFielder11', 'STRING'),
        bigquery.schema.SchemaField('homeFielder12', 'STRING'),
        bigquery.schema.SchemaField('homeBatter1', 'STRING'),
        bigquery.schema.SchemaField('homeBatter2', 'STRING'),
        bigquery.schema.SchemaField('homeBatter3', 'STRING'),
        bigquery.schema.SchemaField('homeBatter4', 'STRING'),
        bigquery.schema.SchemaField('homeBatter5', 'STRING'),
        bigquery.schema.SchemaField('homeBatter6', 'STRING'),
        bigquery.schema.SchemaField('homeBatter7', 'STRING'),
        bigquery.schema.SchemaField('homeBatter8', 'STRING'),
        bigquery.schema.SchemaField('homeBatter9', 'STRING'),
        bigquery.schema.SchemaField('lineupTeamId', 'STRING'),
        bigquery.schema.SchemaField('lineupPlayerId', 'STRING'),
        bigquery.schema.SchemaField('lineupPosition', 'INTEGER'),
        bigquery.schema.SchemaField('lineupOrder', 'INTEGER')
    ]
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