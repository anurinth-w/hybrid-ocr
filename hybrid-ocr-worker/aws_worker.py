import os
import json
import time
import socket
import threading
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import boto3
import psutil
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ConnectionClosedError,
    ReadTimeoutError,
)

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

# OCR simulation / heartbeat
OCR_SIMULATION_SECONDS = int(os.getenv("OCR_SIMULATION_SECONDS", "20"))
WORKER_HEARTBEAT_SECONDS = int(os.getenv("WORKER_HEARTBEAT_SECONDS", "60"))

# CloudWatch metrics
CW_NAMESPACE = os.getenv("CW_NAMESPACE", "HybridOCR")
CW_SERVICE_DIMENSION = os.getenv("CW_SERVICE_DIMENSION", "HybridOCRWorker")

session = boto3.session.Session(region_name=AWS_REGION)

s3 = session.client("s3")
sqs = session.client("sqs")
ddb = session.client("dynamodb")
cw = session.client("cloudwatch")


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def log_event(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "ts": now_ms(),
        "service": "worker",
        "worker_id": WORKER_ID,
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def put_metric(
    name: str,
    value: float,
    unit: str = "Count",
    *,
    extra_dimensions: Optional[list[dict[str, str]]] = None,
) -> None:
    dimensions = [
        {"Name": "Service", "Value": CW_SERVICE_DIMENSION},
    ]
    if extra_dimensions:
        dimensions.extend(extra_dimensions)

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


def get_gpu_metrics() -> tuple[Optional[float], Optional[float]]:
    """
    Returns:
        (gpu_util_percent, gpu_memory_percent)

    Reads first GPU from nvidia-smi output.
    If nvidia-smi is unavailable or GPU is not exposed to the container,
    returns (None, None).
    """
    try:
        result = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            encoding="utf-8",
            stderr=subprocess.DEVNULL,
        ).strip()

        if not result:
            return None, None

        first_line = result.splitlines()[0]
        parts = [p.strip() for p in first_line.split(",")]
        if len(parts) != 3:
            return None, None

        gpu_util = float(parts[0])
        mem_used = float(parts[1])
        mem_total = float(parts[2])

        gpu_mem_percent = (mem_used / mem_total) * 100 if mem_total > 0 else 0.0
        return gpu_util, gpu_mem_percent
    except Exception:
        return None, None


def start_heartbeat(interval_sec: int = 60) -> None:
    def loop() -> None:
        while True:
            try:
                put_metric("WorkerHeartbeat", 1)

                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory().percent

                put_metric("CPUUsagePercent", cpu, "Percent")
                put_metric("MemoryUsagePercent", mem, "Percent")

                gpu_util, gpu_mem_percent = get_gpu_metrics()

                if gpu_util is not None:
                    put_metric("GPUUsagePercent", gpu_util, "Percent")

                if gpu_mem_percent is not None:
                    put_metric("GPUMemoryUsagePercent", gpu_mem_percent, "Percent")

                log_event(
                    "worker_heartbeat",
                    cpu_usage_percent=cpu,
                    memory_usage_percent=mem,
                    gpu_usage_percent=gpu_util,
                    gpu_memory_usage_percent=gpu_mem_percent,
                    heartbeat_interval_sec=interval_sec,
                )
            except Exception as e:
                log_event(
                    "worker_heartbeat_error",
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

            time.sleep(interval_sec)

    t = threading.Thread(target=loop, daemon=True)
    t.start()


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
    Atomic claim: QUEUED -> PROCESSING
    Return (claimed_ok, current_status_if_known)
    """
    t = now_ms()
    try:
        ddb.update_item(
            TableName=DDB_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression=(
                "SET #s=:p, worker_id=:w, last_worker_id=:w, "
                "processing_started_at=:t, updated_at=:t "
                "REMOVE error_code, error_message, failure_type"
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
            status = ddb_get_status(job_id)
            return False, status
        raise


def ddb_set_status(job_id: str, new_status: str, *, extra: Optional[Dict[str, Any]] = None) -> None:
    extra = extra or {}

    expr = "SET #s=:s, updated_at=:u"
    names = {"#s": "status"}
    vals: Dict[str, Dict[str, Any]] = {
        ":s": {"S": new_status},
        ":u": {"N": str(now_ms())},
    }

    for k, v in extra.items():
        if v is None:
            continue
        expr += f", {k}=:{k}"
        if isinstance(v, str):
            vals[f":{k}"] = {"S": v}
        elif isinstance(v, bool):
            vals[f":{k}"] = {"BOOL": v}
        elif isinstance(v, (int, float)):
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


def ddb_release_to_queued(job_id: str, *, reason: str, receive_count: int) -> None:
    if not TRANSIENT_RELEASE_TO_QUEUED:
        return

    t = now_ms()
    try:
        ddb.update_item(
            TableName=DDB_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression=(
                "SET #s=:q, updated_at=:t, last_error_at=:t, "
                "error_code=:c, error_message=:m, failure_type=:ft, "
                "last_receive_count=:rc, last_worker_id=:w"
            ),
            ConditionExpression="#s = :p",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":p": {"S": "PROCESSING"},
                ":q": {"S": "QUEUED"},
                ":t": {"N": str(t)},
                ":c": {"S": "TransientError"},
                ":m": {"S": reason},
                ":ft": {"S": "TRANSIENT"},
                ":rc": {"N": str(receive_count)},
                ":w": {"S": WORKER_ID},
            },
        )
    except ClientError:
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
    if isinstance(exc, RuntimeError) and "PermanentTest" in str(exc):
        return True
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "NotFound", "404"):
            return True
        if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            return True
    return False


def is_transient_error(exc: Exception) -> bool:
    if isinstance(exc, RuntimeError) and "TransientTest" in str(exc):
        return True
    if isinstance(exc, (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError)):
        return True
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
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

    log_event(
        "worker_started",
        aws_region=AWS_REGION,
        queue_url=SQS_URL,
        ddb_table=DDB_TABLE,
        long_poll_seconds=LONG_POLL_SECONDS,
        max_messages=MAX_MESSAGES,
        cw_namespace=CW_NAMESPACE,
        ocr_simulation_seconds=OCR_SIMULATION_SECONDS,
        worker_heartbeat_seconds=WORKER_HEARTBEAT_SECONDS,
    )

    start_heartbeat(WORKER_HEARTBEAT_SECONDS)

    while True:
        resp = sqs.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=MAX_MESSAGES,
            WaitTimeSeconds=LONG_POLL_SECONDS,
            AttributeNames=["ApproximateReceiveCount"],
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        for m in msgs:
            receipt = m["ReceiptHandle"]
            raw_body = m.get("Body", "")
            attrs = m.get("Attributes", {}) or {}
            receive_count = int(attrs.get("ApproximateReceiveCount", "1"))

            log_event(
                "job_received",
                receive_count=receive_count,
                body_preview=raw_body[:200],
            )

            try:
                body = json.loads(raw_body)
                if not isinstance(body, dict):
                    raise ValueError("Body JSON is not an object")
            except Exception as e:
                log_event(
                    "bad_message_body_deleted",
                    receive_count=receive_count,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    body_preview=raw_body[:200],
                )
                put_metric("BadMessagesDeleted", 1)
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                continue

            job_id = body.get("job_id")
            bucket = body.get("bucket")
            input_key = body.get("input_key")
            result_key = body.get("result_key")
            force_error = body.get("force_error")
            created_at = safe_int(body.get("created_at"))

            if not (job_id and bucket and input_key and result_key):
                log_event(
                    "bad_message_fields_deleted",
                    receive_count=receive_count,
                    body=body,
                )
                put_metric("BadMessagesDeleted", 1)
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                continue

            log_event(
                "job_parsed",
                job_id=job_id,
                bucket=bucket,
                input_key=input_key,
                result_key=result_key,
                receive_count=receive_count,
                force_error=force_error,
                created_at=created_at,
            )

            try:
                if s3_object_exists(bucket, result_key):
                    log_event(
                        "job_result_exists",
                        job_id=job_id,
                        bucket=bucket,
                        result_key=result_key,
                        receive_count=receive_count,
                        action="delete_message",
                    )
                    put_metric("ResultAlreadyExists", 1)

                    if MARK_DONE_IF_RESULT_EXISTS:
                        try:
                            done_at = now_ms()
                            end_to_end_latency_ms = None
                            if created_at is not None:
                                end_to_end_latency_ms = max(0, done_at - created_at)
                                put_metric("EndToEndLatencyMs", end_to_end_latency_ms, "Milliseconds")

                            ddb_set_status(
                                job_id,
                                "DONE",
                                extra={
                                    "note": "result_already_exists",
                                    "done_at": done_at,
                                    "end_to_end_latency_ms": end_to_end_latency_ms,
                                    "last_worker_id": WORKER_ID,
                                    "last_receive_count": receive_count,
                                },
                            )
                        except Exception as e:
                            log_event(
                                "ddb_mark_done_if_result_exists_failed",
                                job_id=job_id,
                                error_type=type(e).__name__,
                                error_message=str(e),
                            )

                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                    log_event(
                        "job_deleted_from_queue",
                        job_id=job_id,
                        receive_count=receive_count,
                        reason="result_already_exists",
                    )
                    continue

            except Exception as e:
                log_event(
                    "result_exists_check_failed",
                    job_id=job_id,
                    bucket=bucket,
                    result_key=result_key,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

            try:
                claimed, current_status = claim_job(job_id)
            except Exception as e:
                log_event(
                    "job_claim_error",
                    job_id=job_id,
                    receive_count=receive_count,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )
                raise

            if not claimed:
                if current_status is None:
                    log_event(
                        "job_missing_in_ddb_deleted",
                        job_id=job_id,
                        receive_count=receive_count,
                        action="mark_failed_and_delete",
                    )
                    put_metric("JobsFailed", 1)
                    put_metric("JobsFailedPermanent", 1)
                    try:
                        ddb_set_status(
                            job_id,
                            "FAILED",
                            extra={
                                "error_code": "MissingDDBItem",
                                "error_message": "Message received but DDB item not found",
                                "failure_type": "PERMANENT",
                                "last_worker_id": WORKER_ID,
                                "last_receive_count": receive_count,
                            },
                        )
                    except Exception as e:
                        log_event(
                            "ddb_mark_missing_job_failed_error",
                            job_id=job_id,
                            error_type=type(e).__name__,
                            error_message=str(e),
                        )
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                    log_event(
                        "job_deleted_from_queue",
                        job_id=job_id,
                        receive_count=receive_count,
                        reason="missing_ddb_item",
                    )
                    continue

                if current_status in ("DONE", "FAILED"):
                    log_event(
                        "job_not_claimable_deleted",
                        job_id=job_id,
                        current_status=current_status,
                        receive_count=receive_count,
                    )
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                    log_event(
                        "job_deleted_from_queue",
                        job_id=job_id,
                        receive_count=receive_count,
                        reason=f"not_claimable_{current_status}",
                    )
                    continue

                log_event(
                    "job_not_claimable_visibility_changed",
                    job_id=job_id,
                    current_status=current_status,
                    receive_count=receive_count,
                    visibility_timeout=CLAIM_FAIL_VISIBILITY_DELAY,
                )
                try:
                    sqs.change_message_visibility(
                        QueueUrl=SQS_URL,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=CLAIM_FAIL_VISIBILITY_DELAY,
                    )
                except Exception as e:
                    log_event(
                        "change_visibility_failed",
                        job_id=job_id,
                        receive_count=receive_count,
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                continue

            processing_started_at = now_ms()
            queue_delay_ms = None
            if created_at is not None:
                queue_delay_ms = max(0, processing_started_at - created_at)
                put_metric("QueueDelayMs", queue_delay_ms, "Milliseconds")

            log_event(
                "job_claimed",
                job_id=job_id,
                receive_count=receive_count,
                status="PROCESSING",
                created_at=created_at,
                processing_started_at=processing_started_at,
                queue_delay_ms=queue_delay_ms,
            )

            local_in = Path(f"/tmp/{job_id}.bin")
            local_out = Path(f"/tmp/{job_id}.json")

            try:
                if force_error == "transient":
                    raise RuntimeError("TransientTest")
                if force_error == "permanent":
                    raise RuntimeError("PermanentTest")

                log_event(
                    "job_download_started",
                    job_id=job_id,
                    bucket=bucket,
                    input_key=input_key,
                    receive_count=receive_count,
                )
                s3.download_file(bucket, input_key, str(local_in))
                log_event(
                    "job_download_finished",
                    job_id=job_id,
                    bucket=bucket,
                    input_key=input_key,
                    receive_count=receive_count,
                    local_path=str(local_in),
                )

                log_event(
                    "job_processing_started",
                    job_id=job_id,
                    receive_count=receive_count,
                )

                ocr_started_at = now_ms()

                if OCR_SIMULATION_SECONDS > 0:
                    time.sleep(OCR_SIMULATION_SECONDS)

                result = dummy_ocr(local_in)

                ocr_finished_at = now_ms()
                ocr_duration_ms = max(0, ocr_finished_at - ocr_started_at)

                payload = {
                    "job_id": job_id,
                    "status": "DONE",
                    "duration_ms": ocr_duration_ms,
                    "result": result,
                }
                local_out.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                log_event(
                    "job_upload_started",
                    job_id=job_id,
                    bucket=bucket,
                    result_key=result_key,
                    receive_count=receive_count,
                    ocr_duration_ms=ocr_duration_ms,
                    ocr_simulation_seconds=OCR_SIMULATION_SECONDS,
                )
                s3.upload_file(str(local_out), bucket, result_key)

                done_at = now_ms()
                processing_latency_ms = max(0, done_at - processing_started_at)
                put_metric("ProcessingLatencyMs", processing_latency_ms, "Milliseconds")

                end_to_end_latency_ms = None
                if created_at is not None:
                    end_to_end_latency_ms = max(0, done_at - created_at)
                    put_metric("EndToEndLatencyMs", end_to_end_latency_ms, "Milliseconds")

                ddb_set_status(
                    job_id,
                    "DONE",
                    extra={
                        "duration_ms": ocr_duration_ms,
                        "done_at": done_at,
                        "processing_latency_ms": processing_latency_ms,
                        "end_to_end_latency_ms": end_to_end_latency_ms,
                        "last_worker_id": WORKER_ID,
                        "last_receive_count": receive_count,
                    },
                )

                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                log_event(
                    "job_deleted_from_queue",
                    job_id=job_id,
                    receive_count=receive_count,
                    reason="job_done",
                )

                log_event(
                    "job_processing_completed",
                    job_id=job_id,
                    receive_count=receive_count,
                    status="DONE",
                    result_key=result_key,
                    queue_delay_ms=queue_delay_ms,
                    ocr_duration_ms=ocr_duration_ms,
                    processing_latency_ms=processing_latency_ms,
                    end_to_end_latency_ms=end_to_end_latency_ms,
                )

                put_metric("JobsProcessed", 1)
                put_metric("JobDuration", ocr_duration_ms, "Milliseconds")

            except Exception as e:
                err = f"{type(e).__name__}: {e}"

                if is_permanent_error(e) and not is_transient_error(e):
                    log_event(
                        "job_failed",
                        job_id=job_id,
                        receive_count=receive_count,
                        error_code="PermanentError",
                        error_message=err,
                        failure_type="PERMANENT",
                        action="mark_failed_and_delete",
                    )
                    put_metric("JobsFailed", 1)
                    put_metric("JobsFailedPermanent", 1)

                    try:
                        ddb_set_status(
                            job_id,
                            "FAILED",
                            extra={
                                "error_code": "PermanentError",
                                "error_message": err,
                                "failure_type": "PERMANENT",
                                "last_receive_count": receive_count,
                                "last_worker_id": WORKER_ID,
                            },
                        )
                    except Exception as update_err:
                        log_event(
                            "ddb_mark_failed_error",
                            job_id=job_id,
                            receive_count=receive_count,
                            error_type=type(update_err).__name__,
                            error_message=str(update_err),
                        )

                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt)
                    log_event(
                        "job_deleted_from_queue",
                        job_id=job_id,
                        receive_count=receive_count,
                        reason="permanent_failure",
                    )

                else:
                    log_event(
                        "job_failed",
                        job_id=job_id,
                        receive_count=receive_count,
                        error_code="TransientError",
                        error_message=err,
                        failure_type="TRANSIENT",
                        action="release_to_queued",
                    )
                    put_metric("JobsFailed", 1)
                    put_metric("JobsFailedTransient", 1)
                    ddb_release_to_queued(job_id, reason=err, receive_count=receive_count)
                    # DO NOT delete message => SQS retry / DLQ after threshold

            finally:
                try:
                    local_in.unlink(missing_ok=True)
                except Exception as e:
                    log_event(
                        "cleanup_local_input_failed",
                        job_id=job_id,
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                try:
                    local_out.unlink(missing_ok=True)
                except Exception as e:
                    log_event(
                        "cleanup_local_output_failed",
                        job_id=job_id,
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )


if __name__ == "__main__":
    main()
