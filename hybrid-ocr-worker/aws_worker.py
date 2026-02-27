import os, json, time, socket
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
SQS_URL = os.getenv("OCR_SQS_URL")
DDB_TABLE = os.getenv("OCR_DDB_TABLE")

WORKER_ID = socket.gethostname()

session = boto3.session.Session(region_name=AWS_REGION)
s3 = session.client("s3")
sqs = session.client("sqs")
ddb = session.client("dynamodb")

def now_ms() -> int:
    return int(time.time() * 1000)

def claim_job(job_id: str) -> bool:
    """
    Atomic job claim:
    Only one worker can transition QUEUED -> PROCESSING.
    """
    t = now_ms()
    try:
        ddb.update_item(
            TableName=DDB_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="SET #s=:p, worker_id=:w, processing_started_at=:t, updated_at=:t",
            ConditionExpression="#s = :q",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":q": {"S": "QUEUED"},
                ":p": {"S": "PROCESSING"},
                ":w": {"S": WORKER_ID},
                ":t": {"N": str(t)},
            },
        )
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return False
        raise

def ddb_set_status(job_id: str, new_status: str, *, extra: dict | None = None):
    """
    General status update. Does NOT overwrite worker_id.
    worker_id should be written only during claim_job().
    """
    extra = extra or {}
    expr = "SET #s=:s, updated_at=:u"
    names = {"#s": "status"}
    vals = {
        ":s": {"S": new_status},
        ":u": {"N": str(now_ms())},
    }

    # extra fields
    for k, v in extra.items():
        if v is None:
            continue
        expr += f", {k}=:{k}"
        if isinstance(v, str):
            vals[f":{k}"] = {"S": v}
        elif isinstance(v, int):
            vals[f":{k}"] = {"N": str(v)}
        else:
            vals[f":{k}"] = {"S": json.dumps(v, ensure_ascii=False)}

    ddb.update_item(
        TableName=DDB_TABLE,
        Key={"job_id": {"S": job_id}},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )

def s3_object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def dummy_ocr(local_in_path: Path) -> dict:
    # Phase 1: แค่พิสูจน์ระบบไหลครบวงจร
    size = local_in_path.stat().st_size if local_in_path.exists() else 0
    return {
        "text": "dummy",
        "pages": 1,
        "input_size_bytes": size,
    }

def main():
    if not (SQS_URL and DDB_TABLE):
        raise SystemExit("Missing env: OCR_SQS_URL and/or OCR_DDB_TABLE")

    print(f"[worker] started worker_id={WORKER_ID} region={AWS_REGION}")
    while True:
        resp = sqs.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,  # long polling
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        m = msgs[0]
        receipt = m["ReceiptHandle"]
        body = json.loads(m["Body"])

        job_id = body["job_id"]
        bucket = body["bucket"]
        input_key = body["input_key"]
        result_key = body["result_key"]

        # idempotency: ถ้า result มีแล้ว แปลว่างานเคยสำเร็จแล้ว
        try:
            if s3_object_exists(bucket, result_key):
                print(f"[worker] job {job_id} result exists -> delete msg")
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                continue
        except Exception as e:
            # infra read error; do not treat as claimed/not-claimed
            print(f"[worker] head_object error: {type(e).__name__}: {e}")

        # atomic claim
        try:
            ok = claim_job(job_id)
        except Exception as e:
            # Real infra error (permission/outage/etc). Fail loud.
            print(f"[worker] claim error job {job_id}: {type(e).__name__}: {e}")
            raise

        if not ok:
            print(f"[worker] job {job_id} already claimed/not QUEUED -> delete msg")
            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
            continue

        local_in = Path(f"/tmp/{job_id}.bin")
        local_out = Path(f"/tmp/{job_id}.json")

        try:
            print(f"[worker] job {job_id} downloading s3://{bucket}/{input_key}")
            s3.download_file(bucket, input_key, str(local_in))

            t0 = now_ms()
            result = dummy_ocr(local_in)
            duration = now_ms() - t0

            payload = {
                "job_id": job_id,
                "status": "DONE",
                "duration_ms": duration,
                "result": result,
            }
            local_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            print(f"[worker] job {job_id} uploading result s3://{bucket}/{result_key}")
            s3.upload_file(str(local_out), bucket, result_key)

            ddb_set_status(job_id, "DONE", extra={"duration_ms": duration})
            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
            print(f"[worker] job {job_id} DONE")

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            print(f"[worker] job {job_id} FAILED: {err}")
            try:
                ddb_set_status(job_id, "FAILED", extra={"error_message": err})
            except Exception:
                pass
            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)

        finally:
            try:
                local_in.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                local_out.unlink(missing_ok=True)
            except Exception:
                pass

if __name__ == "__main__":
    main()
