# app.py

import os
import uuid
import time
import json
from typing import Any

from flask import Flask, request, jsonify

import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

API_KEY = os.getenv("OCR_API_KEY", "")

S3_BUCKET = os.getenv("OCR_S3_BUCKET")
SQS_URL = os.getenv("OCR_SQS_URL")
DDB_TABLE = os.getenv("OCR_DDB_TABLE")
AWS_REGION = os.getenv("AWS_REGION")

CW_NAMESPACE = os.getenv("CW_NAMESPACE", "HybridOCR")
CW_SERVICE_DIMENSION = os.getenv("CW_SERVICE_DIMENSION", "HybridOCRAPI")

session = boto3.session.Session(region_name=AWS_REGION)

s3 = session.client("s3")
sqs = session.client("sqs")
ddb = session.client("dynamodb")
cw = session.client("cloudwatch")


def now_ms() -> int:
    return int(time.time() * 1000)


def log_event(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "ts": now_ms(),
        "service": "api",
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def put_metric(
    name: str,
    value: float,
    unit: str = "Count",
) -> None:
    dimensions = [
        {"Name": "Service", "Value": CW_SERVICE_DIMENSION},
    ]

    try:
        cw.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": dimensions,
                    "Timestamp": int(time.time()),
                }
            ],
        )
    except Exception as e:
        log_event(
            "cloudwatch_metric_error",
            metric_name=name,
            metric_value=value,
            metric_unit=unit,
            error_type=type(e).__name__,
            error_message=str(e),
        )


def require_api_key(req) -> bool:
    if not API_KEY:
        return False
    return req.headers.get("x-api-key", "") == API_KEY


def validate_file(f):
    allowed = {"application/pdf", "image/png", "image/jpeg"}
    if f.mimetype not in allowed:
        return False, f"unsupported content-type: {f.mimetype}"

    f.seek(0, 2)
    size = f.tell()
    f.seek(0)

    if size > 25 * 1024 * 1024:
        return False, "file too large (max 25MB)"

    return True, None


@app.post("/jobs")
def create_job():
    if not require_api_key(request):
        return jsonify({"error": "unauthorized"}), 401

    if not (S3_BUCKET and SQS_URL and DDB_TABLE and AWS_REGION):
        return jsonify({"error": "server misconfigured (missing env)"}), 500

    if "file" not in request.files:
        return jsonify({"error": "file is required (multipart field: file)"}), 400

    f = request.files["file"]
    ok, err = validate_file(f)
    if not ok:
        return jsonify({"error": err}), 400

    job_id = str(uuid.uuid4())
    filename = (f.filename or "upload").replace("/", "_")
    input_key = f"uploads/{job_id}/{filename}"
    result_key = f"results/{job_id}/result.json"
    ts = now_ms()

    log_event(
        "job_create_requested",
        job_id=job_id,
        filename=filename,
        content_type=f.mimetype,
        created_at=ts,
    )

    # 1) upload to S3
    try:
        s3.upload_fileobj(f, S3_BUCKET, input_key)
    except ClientError as e:
        put_metric("JobCreateFailures", 1)
        log_event(
            "job_create_failed",
            job_id=job_id,
            status="FAILED",
            error_type="S3UploadFailed",
            error_message=str(e),
        )
        return jsonify({"error": "s3 upload failed", "detail": str(e)}), 500

    # 2) put DynamoDB (QUEUED)
    try:
        ddb.put_item(
            TableName=DDB_TABLE,
            Item={
                "job_id": {"S": job_id},
                "status": {"S": "QUEUED"},
                "input_s3_key": {"S": input_key},
                "result_s3_key": {"S": result_key},
                "original_filename": {"S": filename},
                "content_type": {"S": f.mimetype},
                "created_at": {"N": str(ts)},
                "updated_at": {"N": str(ts)},
            },
            ConditionExpression="attribute_not_exists(job_id)",
        )
    except ClientError as e:
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=input_key)
        except Exception:
            pass

        put_metric("JobCreateFailures", 1)
        log_event(
            "job_create_failed",
            job_id=job_id,
            status="FAILED",
            error_type="DDBPutFailed",
            error_message=str(e),
        )
        return jsonify({"error": "ddb put failed", "detail": str(e)}), 500

    # 3) send SQS
    msg = {
        "job_id": job_id,
        "bucket": S3_BUCKET,
        "input_key": input_key,
        "result_key": result_key,
        "created_at": ts,
    }

    try:
        sqs.send_message(QueueUrl=SQS_URL, MessageBody=json.dumps(msg))
    except ClientError as e:
        ddb.update_item(
            TableName=DDB_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="SET #s=:s, error_message=:e, updated_at=:u",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": {"S": "FAILED"},
                ":e": {"S": f"sqs send failed: {e}"},
                ":u": {"N": str(now_ms())},
            },
        )

        put_metric("JobCreateFailures", 1)
        log_event(
            "job_create_failed",
            job_id=job_id,
            status="FAILED",
            error_type="SQSSendFailed",
            error_message=str(e),
        )
        return jsonify({"error": "sqs send failed", "job_id": job_id}), 500

    put_metric("JobsCreated", 1)
    log_event(
        "job_created",
        job_id=job_id,
        status="QUEUED",
        input_s3_key=input_key,
        result_s3_key=result_key,
        created_at=ts,
    )

    return jsonify({"job_id": job_id, "status": "QUEUED"}), 201


@app.get("/jobs/<job_id>")
def get_job(job_id):
    if not require_api_key(request):
        return jsonify({"error": "unauthorized"}), 401

    resp = ddb.get_item(TableName=DDB_TABLE, Key={"job_id": {"S": job_id}})
    if "Item" not in resp:
        return jsonify({"error": "not found"}), 404

    it = resp["Item"]
    out = {
        "job_id": job_id,
        "status": it["status"]["S"],
        "input_s3_key": it.get("input_s3_key", {}).get("S"),
        "result_s3_key": it.get("result_s3_key", {}).get("S"),
        "error_message": it.get("error_message", {}).get("S"),
        "updated_at": int(it.get("updated_at", {}).get("N", "0")),
    }

    if out["status"] == "DONE" and out["result_s3_key"]:
        out["result_download_url"] = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": out["result_s3_key"]},
            ExpiresIn=3600,
        )

    return jsonify(out), 200


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
