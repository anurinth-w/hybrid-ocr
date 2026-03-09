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
