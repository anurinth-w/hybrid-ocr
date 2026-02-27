Hybrid OCR Infrastructure
Production-Inspired Asynchronous Processing System (AWS)

Overview

This project is a production-inspired asynchronous OCR processing system built on AWS using:

-Amazon S3

-Amazon SQS (Standard Queue)

-Amazon DynamoDB

It demonstrates:

-Decoupled architecture

-At-least-once delivery handling

-Idempotent worker design

-Atomic concurrency control using DynamoDB conditional updates

-Failure-aware state transitions

-Dockerized services

-Local orchestration via Docker Compose

The focus of this project is infrastructure reliability and distributed system behavior rather than OCR accuracy.
--------------------------------------------------------------

Architecture Flow

Client
|
v
Flask API
|
|-- Upload file -> S3
|-- Create job (QUEUED) -> DynamoDB
|-- Send message -> SQS
|
v
Worker (poll SQS)
|
|-- Atomic job claim (QUEUED -> PROCESSING)
|-- Download input -> S3
|-- Run OCR
|-- Upload result -> S3
|-- Update job (DONE / FAILED) -> DynamoDB
|-- Delete message -> SQS


--------------------------------------------------------------

Core Design Principles

DynamoDB is the source of truth

S3 stores immutable input and result artifacts

SQS provides asynchronous decoupling (at-least-once delivery)

Worker processing is idempotent

System is safe against crash and duplicate delivery scenarios

--------------------------------------------------------------

Concurrency Control

SQS Standard Queue provides at-least-once delivery.
This means multiple workers may receive the same message.

To prevent duplicate processing, the system implements atomic job claiming
using DynamoDB conditional updates.

Only one worker can transition:

QUEUED -> PROCESSING

using a conditional expression on the status attribute.

If the condition fails, another worker has already claimed the job,
and the message is safely discarded.

This prevents race conditions without requiring a separate lock service.

--------------------------------------------------------------

Idempotency Strategy

The worker is safe under duplicate message delivery:

Result S3 keys are deterministic (results/<job_id>/result.json)

If result already exists, the worker skips processing

DynamoDB state transitions are guarded with conditional writes

Even if:

A worker crashes before deleting the SQS message

Visibility timeout expires

Another worker receives the same message

The system remains consistent.

--------------------------------------------------------------

Failure Handling

This system handles:

-Worker crash before delete_message

-Crash after result upload but before DynamoDB update

-Duplicate message delivery

-Conditional write conflicts

DynamoDB conditional expressions prevent invalid state transitions.

--------------------------------------------------------------

Tech Stack
-Python 3.11
-Flask
-Gunicorn
-boto3
-AWS S3
-AWS SQS
-AWS DynamoDB
-Docker
-Docker Compose

--------------------------------------------------------------

Project Structure

hybrid-ocr/
|
|-- hybrid-ocr-api/
| |-- app.py
| |-- Dockerfile
| |-- requirements.txt
|
|-- hybrid-ocr-worker/
| |-- aws_worker.py
| |-- Dockerfile
| |-- requirements.txt
|
|-- docker-compose.yml
|-- .env.example
|-- README.md

--------------------------------------------------------------

Environment Variables

Create a .env file in the project root:

OCR_API_KEY=changeme
OCR_S3_BUCKET=your-bucket-name
OCR_SQS_URL=https://sqs.ap-southeast-1.amazonaws.com/ACCOUNT_ID/queue-name
OCR_DDB_TABLE=your-ddb-table
AWS_REGION=ap-southeast-1

--------------------------------------------------------------

Running Locally

Build and start both services:
docker compose up --build
API will be available at:
http://localhost:8000
Health check:
curl http://localhost:8000/health

--------------------------------------------------------------
--------------------------------------------------------------

Author

Anurinth Wichairum
Cloud / DevOps Engineer (Aspiring)
