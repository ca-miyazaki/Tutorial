import base64
import json
import subprocess

from flask import Flask, request

app = Flask(__name__)

# required
PROJECT_ID = 'ca-miyazaki-test'
DATASET_NAME = 'etl_training_dataset'
# optional
TABLE_NAME = 'raw_data'


@app.route("/", methods=["POST"])
def main():
    data = json.loads(base64.b64decode(request.get_json()["message"]["data"]))
    bucket = data["bucket"]
    name = data["name"]
    print("Start load CSV on gs://{}/{} to BigQuery table {}.{}.{}".format(
        bucket, name, PROJECT_ID, DATASET_NAME, TABLE_NAME))

    # BQ コマンド実行
    exit_status = subprocess.call([
        "bq", "--project_id", PROJECT_ID, "load",
        "--autodetect",
        "--replace",
        "--source_format", "CSV",
        f"{DATASET_NAME}.{TABLE_NAME}",
        f"gs://{bucket}/{name}"])

    if exit_status != 0:
        # Cloud Run 上で実行するコードが crush すると、Pub/Sub message が ack されない (レスポンスコード 500 が返るため)。
        # リトライされ続けることを回避するため、例外は発生させない。
        print("[ERROR] Failed to load CSV.")
    else:
        print("Successfully load CSV to BigQuery.")

    return "Finished", 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))