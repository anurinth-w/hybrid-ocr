output "bucket_name" {
  value = module.s3.bucket_name
}

output "bucket_arn" {
  value = module.s3.bucket_arn
}

output "queue_name" {
  value = module.sqs.queue_name
}

output "queue_url" {
  value = module.sqs.queue_url
}

output "dlq_name" {
  value = module.sqs.dlq_name
}

output "dlq_url" {
  value = module.sqs.dlq_url
}

output "dynamodb_table_name" {
  value = module.dynamodb.table_name
}

output "dynamodb_table_arn" {
  value = module.dynamodb.table_arn
}

output "api_policy_arn" {
  value = module.iam.api_policy_arn
}

output "worker_policy_arn" {
  value = module.iam.worker_policy_arn
}

output "api_repository_name" {
  value = module.ecr.api_repository_name
}

output "api_repository_url" {
  value = module.ecr.api_repository_url
}

output "worker_repository_name" {
  value = module.ecr.worker_repository_name
}

output "worker_repository_url" {
  value = module.ecr.worker_repository_url
}
