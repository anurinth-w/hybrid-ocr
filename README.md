Hybrid OCR Infrastructure Production-Inspired Asynchronous Processing
System (AWS)


Overview

This project is a Production-inspired asynchronous OCR processing system built using AWS S3, SQS, and DynamoDB.

It demonstrates:

-   Decoupled architecture
-   At-least-once delivery handling
-   Idempotent worker design
-   Conditional writes in DynamoDB
-   Failure-aware state transitions
-   Dockerized services
-   Local orchestration via Docker Compose

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
|-- Process OCR
|-- Upload result -> S3
|-- Update job (DONE / FAILED) -> DynamoDB
|-- Delete message -> SQS

Design principles:
-DynamoDB is the source of truth
-S3 stores input and result files
-SQS provides async decoupling
-Worker is idempotent
-Safe against crash scenarios

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

API Endpoints

Create Job

POST /jobs
Header: x-api-key: <OCR_API_KEY>
Content-Type: multipart/form-data
Field: file

Response:

{
"job_id": "...",
"status": "QUEUED"
}

--------------------------------------------------------------

Get Job Status

GET /jobs/<job_id>
Header: x-api-key: <OCR_API_KEY>

Possible states:

-QUEUED
-PROCESSING
-DONE
-FAILED

If DONE, a presigned S3 download URL is returned.

--------------------------------------------------------------

Failure Handling

This system handles:
-At-least-once delivery (SQS Standard)
-Worker crash before delete_message
-Crash after result upload but before DynamoDB update
-Idempotent job execution
DynamoDB conditional expressions prevent duplicate state transitions.

------------------------------------------------------------

Engineering Concepts Demonstrated

-Asynchronous processing
-Decoupled architecture
-Message-driven systems
-Visibility timeout awareness
-Idempotent design
-Production-style containerization

------------------------------------------------------------

Author

Anurinth Wichairum
Cloud / DevOps Engineer (Aspiring)
