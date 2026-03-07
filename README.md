Hybrid OCR Infrastructure

A distributed OCR processing system designed to simulate production-grade asynchronous job processing using AWS primitives.

This project demonstrates how to build a reliable worker-based architecture with retry handling, failure classification, and dead-letter queue (DLQ) strategy.

The goal of this project is to practice system design concepts used in backend / DevOps / platform engineering roles.

----------------------------------------------------------------------------------------------

Architecture Overview
Client
  ↓
API (Flask / Gunicorn)
  ↓
S3 (store uploaded files)
  ↓
DynamoDB (source of truth for job state)
  ↓
SQS (job queue)
  ↓
Worker
  ↓
S3 (store OCR result)

System components:

-API – accepts file uploads and creates OCR jobs
-S3 – stores input documents and OCR results
-DynamoDB – tracks job state
-SQS – queues jobs for asynchronous processing
-Worker – processes jobs and performs OCR

----------------------------------------------------------------------------------------------

Job Lifecycle

Each job moves through a defined state machine:

QUEUED
  ↓
PROCESSING
  ↓
DONE

Failure paths:

PROCESSING
  ↓
FAILED (permanent error)

or

PROCESSING
  ↓
QUEUED (transient error → retry)
  ↓
DLQ (after maxReceiveCount)

----------------------------------------------------------------------------------------------

Failure Handling Strategy

The worker classifies failures into two categories.

Permanent Errors

Examples:

-Invalid input
-Corrupt files
-Logic errors

Behavior:

  PROCESSING → FAILED
  SQS message deleted

----------------------------------------------------------------------------------------------

Transient Errors

Examples:

-Network timeouts
-Temporary AWS service errors

Behavior:

  PROCESSING → QUEUED
  Message not deleted
  SQS retries automatically

If retries exceed maxReceiveCount, the message moves to the Dead Letter Queue (DLQ).

----------------------------------------------------------------------------------------------

Idempotency

The system implements idempotent job processing.

If the result file already exists in S3:

worker detects existing result
→ skips processing
→ deletes message

This prevents duplicate work when messages are retried.

----------------------------------------------------------------------------------------------

Local Development

Run the system locally using Docker:

  docker compose up --build

API will start on:

  http://localhost:8000

----------------------------------------------------------------------------------------------

Create a Job

Upload a document:

  curl -X POST http://localhost:8000/jobs   -H "x-api-key: changeme"   -F "file=@document.pdf"

Response:

  {
    "job_id": "uuid",
    "status": "QUEUED"
  }

----------------------------------------------------------------------------------------------

Worker Processing Flow

Worker steps:

1.Receive message from SQS
2.Atomically claim job in DynamoDB
3.Download input file from S3
4.Run OCR
5.Upload result to S3
6.Update job status in DynamoDB

----------------------------------------------------------------------------------------------

Test Scenarios

The system was tested with three scenarios.

Success
  QUEUED → PROCESSING → DONE

Permanent Failure
  PROCESSING → FAILED
  message deleted

Transient Failure
  PROCESSING → QUEUED
  retry
  DLQ after maxReceiveCount

----------------------------------------------------------------------------------------------

Observability

The worker emits structured JSON logs for every important lifecycle event.

This allows logs to be easily consumed by systems such as:
-AWS CloudWatch
-ELK stack
-Datadog
-OpenTelemetry collectors

Example log event:
{
  "event": "job_claimed",
  "ts": 1772895586088,
  "worker_id": "e44958831c37",
  "job_id": "60ba891b-7f39-40f1-9b43-833519f008ed",
  "receive_count": 1,
  "status": "PROCESSING"
}

Key lifecycle events emitted by the worker:
-message_received
-job_parsed
-job_claimed
-job_download_started
-job_download_finished
-job_processing_started
-job_upload_started
-job_done
-job_failed_permanent
-job_failed_transient

These logs make it possible to reconstruct the timeline of a job execution, which is useful for debugging, monitoring, and production observability.

----------------------------------------------------------------------------------------------

Technology Stack

-Python
-Flask
-Docker
-AWS S3
-AWS SQS
-AWS DynamoDB

----------------------------------------------------------------------------------------------

Learning Goals

This project focuses on understanding:

-distributed worker systems
-retry policies
-DLQ architecture
-idempotent processing
-atomic job claiming
-failure classification

----------------------------------------------------------------------------------------------

Future Improvements

Planned improvements:

-structured JSON logging
-CloudWatch metrics and alarms
-Terraform infrastructure provisioning
-worker autoscaling
-Kubernetes deployment

----------------------------------------------------------------------------------------------

Author

Anurinth Wichairum
