variable "project" {
  type    = string
  default = "hybrid-ocr"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
}

variable "bucket_force_destroy" {
  type    = bool
  default = true
}

variable "visibility_timeout_seconds" {
  type    = number
  default = 120
}

variable "message_retention_seconds" {
  type    = number
  default = 345600
}

variable "receive_wait_time_seconds" {
  type    = number
  default = 10
}

variable "max_receive_count" {
  type    = number
  default = 3
}
