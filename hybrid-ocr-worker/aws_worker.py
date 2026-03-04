import os
import json
import time
import socket
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError, ReadTimeoutError

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
SQS_URL = os.getenv("OCR_SQS_URL")
DDB_TABLE = os.getenv("OCR_DDB_TABLE")

WORKER_ID = socket.gethostname()

# Tuning knobs
LONG_POLL_SECONDS = int(os.getenv("WORKER_LONG_POLL_SECONDS", "20"))
MAX_MESSAGES = int(os.getenv("WORKER_MAX_MESSAGES", "1"))
CLAIM_FAIL_VISIBILITY_DELAY = int(os.getenv("WORKER_CLAIM_FAIL_VIS_DELAY", "30"))  # seconds
TRANSIENT_RELEASE_TO_QUEUED = os.getenv("WORKER_TRANSIENT_RELEASE_TO_QUEUED", "1") == "1"
MARK_DONE_IF_RESULT_EXISTS = os.getenv("WORKER_MARK_DONE_IF_RESULT_EXISTS", "1") == "1"

session = boto3.session.Session(region_name=AWS_REGION)
s3 = session.client("s3")
sqs = session.client("sqs")
ddb = session.client("dynamodb")


def now_ms() -> int:
    return int(time.time() * 1000)


# -------------------------
# DynamoDB helpers
# -------------------------

def ddb_get_status(job_id: str) -> Optional[str]:
    """Return status string if item exists, else None."""
    resp = ddb.get_item(
        TableName=DDB_TABLE,
        Key={"job_id": {"S": job_id}},
        ConsistentRead=True,
        ProjectionExpression="#s",
        ExpressionAttributeNames={"#s": "status"},
    )
    item = resp.get("Item")
    if not item:
        return None
    return item.get("status", {}).get("S")


def claim_job(job_id: str) -> Tuple[bool, Optional[str]]:
    """
    Atomic claim:
      QUEUED -> PROCESSING
    Return (claimed_ok, current_status_if_known)
    """
    t = now_ms()
    try:
        ddb.update_item(
            TableName=DDB_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression=(
                "SET #s=:p, worker_id=:w, processing_started_at=:t, updated_at=:t "
                "REMOVE error_code, error_message"
            ),
            ConditionExpression="#s = :q",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":q": {"S": "QUEUED"},
                ":p": {"S": "PROCESSING"},
                ":w": {"S": WORKER_ID},
                ":t": {"N": str(t)},
            },
        )
        return True, "PROCESSING"
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            # Either item missing OR status != QUEUED
            status = ddb_get_status(job_id)
            return False, status
        raise


def ddb_set_status(job_id: str, new_status: str, *, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    General status update. Does NOT overwrite worker_id by default.
    """
    extra = extra or {}
    expr = "SET #s=:s, updated_at=:u"
    names = {"#s": "status"}
    vals: Dict[str, Dict[str, str]] = {
        ":s": {"S": new_status},
        ":u": {"N": str(now_ms())},
    }

    for k, v in extra.items():
        if v is None:
            continue
        # Note: attribute names are assumed safe here; keep to snake_case keys.
        expr += f", {k}=:{k}"
        if isinstance(v, str):
            vals[f":{k}"] = {"S": v}
        elif isinstance(v, (int, float)):
            vals[f":{k}"] = {"N": str(int(v))}
        else:
            vals[f":{k}"] = {"S": json.dumps(v, ensure_ascii=False)}

    ddb.update_item(
        TableName=DDB_TABLE,
        Key={"job_id": {"S": job_id}},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )


def ddb_release_to_queued(job_id: str, *, reason: str, receive_count: int) -> None:
    """
    On transient error:
      PROCESSING -> QUEUED (so future retry can claim again)
    We do NOT condition on worker_id to keep it simple in dev,
    but we do condition that status is PROCESSING (avoid clobbering DONE).
    """
    if not TRANSIENT_RELEASE_TO_QUEUED:
        return

    t = now_ms()
    try:
        ddb.update_item(
            TableName=DDB_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="SET #s=:q, updated_at=:t, last_error_at=:t, error_code=:c, error_message=:m, last_receive_count=:rc",
            ConditionExpression="#s = :p",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":p": {"S": "PROCESSING"},
                ":q": {"S": "QUEUED"},
                ":t": {"N": str(t)},
                ":c": {"S": "TransientError"},
                ":m": {"S": reason},
                ":rc": {"N": str(receive_count)},
            },
        )
    except ClientError:
        # Don't die on release failure; worst case it stays PROCESSING and will be handled by future recovery.
        pass


# -------------------------
# S3 helpers
# -------------------------

def s3_object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# -------------------------
# Error classification
# -------------------------

def is_permanent_error(exc: Exception) -> bool:
    # Your own explicit test hook
    if isinstance(exc, RuntimeError) and "PermanentTest" in str(exc):
        return True

    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        # S3 missing input = permanent (bad key or missing upload)
        if code in ("NoSuchKey", "NotFound", "404"):
            return True
        # AccessDenied is usually config/permission (treat as permanent for now)
        if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            return True

    return False


def is_transient_error(exc: Exception) -> bool:
    # Your explicit test hook
    if isinstance(exc, RuntimeError) and "TransientTest" in str(exc):
        return True

    # Network-ish errors
    if isinstance(exc, (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError)):
        return True

    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        # Throttling / timeouts / temporary service issues
        if code in (
            "Throttling",
            "ThrottlingException",
            "ProvisionedThroughputExceededException",
            "RequestTimeout",
            "RequestTimeoutException",
            "InternalError",
            "ServiceUnavailable",
        ):
            return True
        # Many 5xx come as "500" style or "SlowDown" for S3
        if code in ("SlowDown",) or code.startswith("5"):
            return True

    return False


# -------------------------
# OCR (dummy)
# -------------------------

def dummy_ocr(local_in_path: Path) -> Dict[str, Any]:
    size = local_in_path.stat().st_size if local_in_path.exists() else 0
    return {"text": "dummy", "pages": 1, "input_size_bytes": size}


# -------------------------
# Worker loop
# -------------------------

def main() -> None:
    if not (SQS_URL and DDB_TABLE):
        raise SystemExit("Missing env: OCR_SQS_URL and/or OCR_DDB_TABLE")

    print(f"[worker] started worker_id={WORKER_ID} region={AWS_REGION}")

    while True:
        resp = sqs.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=MAX_MESSAGES,
            WaitTimeSeconds=LONG_POLL_SECONDS,
            AttributeNames=["ApproximateReceiveCount"],  # ✅ correct spelling
        )

        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        for m in msgs:
            receipt = m["ReceiptHandle"]
            raw_body = m.get("Body", "")
            attrs = m.get("Attributes", {}) or {}
            receive_count = int(attrs.get("ApproximateReceiveCount", "1"))

            # Parse JSON safely; if bad message, delete it (poison)
            try:
                body = json.loads(raw_body)
                if not isinstance(body, dict):
                    raise ValueError("Body JSON is not an object")
            except Exception as e:
                print(f"[worker] BAD_MESSAGE_BODY -> delete msg rc={receive_count} err={type(e).__name__}: {e} body_preview={raw_body[:200]!r}")
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                continue

            # Required fields
            job_id = body.get("job_id")
            bucket = body.get("bucket")
            input_key = body.get("input_key")
            result_key = body.get("result_key")
            force_error = body.get("force_error")  # optional: transient/permanent

            if not (job_id and bucket and input_key and result_key):
                print(f"[worker] BAD_MESSAGE_FIELDS -> delete msg rc={receive_count} body={body}")
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                continue

            # 1) Idempotency shortcut: if result exists, delete msg (and optionally mark DONE)
            try:
                if s3_object_exists(bucket, result_key):
                    print(f"[worker] job {job_id} result exists -> delete msg rc={receive_count}")
                    if MARK_DONE_IF_RESULT_EXISTS:
                        try:
                            ddb_set_status(job_id, "DONE", extra={"note": "result_already_exists"})
                        except Exception:
                            pass
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                    continue
            except Exception as e:
                print(f"[worker] job {job_id} head_object error (non-fatal): {type(e).__name__}: {e}")

            # 2) Atomic claim
            try:
                claimed, current_status = claim_job(job_id)
            except Exception as e:
                # Infra error: better to fail loud (dev) so you see it
                print(f"[worker] claim error job {job_id}: {type(e).__name__}: {e}")
                raise

            if not claimed:
                # If item missing: poison message -> mark FAILED + delete
                if current_status is None:
                    print(f"[worker] job {job_id} missing in DDB -> FAILED(permanent) + delete msg rc={receive_count}")
                    try:
                        ddb_set_status(job_id, "FAILED", extra={"error_code": "MissingDDBItem", "error_message": "Message received but DDB item not found"})
                    except Exception:
                        pass
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                    continue

                # If not QUEUED (e.g., PROCESSING/DONE/FAILED):
                # - DONE/FAILED: safe to delete msg
                if current_status in ("DONE", "FAILED"):
                    print(f"[worker] job {job_id} status={current_status} -> delete msg rc={receive_count}")
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                    continue

                # - PROCESSING or others: DO NOT delete (let it retry or wait)
                print(f"[worker] job {job_id} status={current_status} (not claimable) -> keep msg rc={receive_count} (delay {CLAIM_FAIL_VISIBILITY_DELAY}s)")
                try:
                    sqs.change_message_visibility(
                        QueueUrl=SQS_URL,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=CLAIM_FAIL_VISIBILITY_DELAY,
                    )
                except Exception:
                    pass
                continue

            # Claimed OK: proceed
            local_in = Path(f"/tmp/{job_id}.bin")
            local_out = Path(f"/tmp/{job_id}.json")

            try:
                # Test hooks: controlled error injection
                if force_error == "transient":
                    raise RuntimeError("TransientTest")
                if force_error == "permanent":
                    raise RuntimeError("PermanentTest")

                print(f"[worker] job {job_id} downloading s3://{bucket}/{input_key} rc={receive_count}")
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

                print(f"[worker] job {job_id} uploading result s3://{bucket}/{result_key} rc={receive_count}")
                s3.upload_file(str(local_out), bucket, result_key)

                ddb_set_status(job_id, "DONE", extra={"duration_ms": duration})
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                print(f"[worker] job {job_id} DONE rc={receive_count}")

            except Exception as e:
                err = f"{type(e).__name__}: {e}"

                if is_permanent_error(e) and not is_transient_error(e):
                    # Permanent: mark FAILED and delete
                    print(f"[worker] job {job_id} FAILED(permanent) rc={receive_count}: {err} -> delete msg")
                    try:
                        ddb_set_status(job_id, "FAILED", extra={"error_code": "PermanentError", "error_message": err, "last_receive_count": receive_count})
                    except Exception:
                        pass
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)

                else:
                    # Default to transient if not clearly permanent
                    print(f"[worker] job {job_id} FAILED(transient) rc={receive_count}: {err} -> keep msg for retry (DLQ after maxReceiveCount)")
                    ddb_release_to_queued(job_id, reason=err, receive_count=receive_count)
                    # DO NOT delete message => SQS will retry / DLQ after threshold

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
