module "labels" {
  source      = "../../modules/labels"
  project     = var.project
  environment = var.environment
}

module "s3" {
  source            = "../../modules/s3"
  bucket_name       = "${module.labels.prefix}-storage"
  force_destroy     = var.bucket_force_destroy
  enable_versioning = true
  common_tags       = module.labels.common_tags
}

module "sqs" {
  source                     = "../../modules/sqs"
  queue_name                 = "${module.labels.prefix}-jobs"
  dlq_name                   = "${module.labels.prefix}-jobs-dlq"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = var.receive_wait_time_seconds
  max_receive_count          = var.max_receive_count
  common_tags                = module.labels.common_tags
}

module "dynamodb" {
  source       = "../../modules/dynamodb"
  table_name   = "${module.labels.prefix}-jobs"
  hash_key     = "job_id"
  billing_mode = "PAY_PER_REQUEST"
  enable_pitr  = true
  common_tags  = module.labels.common_tags
}
