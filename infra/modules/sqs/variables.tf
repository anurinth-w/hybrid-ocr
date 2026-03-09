variable "queue_name" {
  type = string
}

variable "dlq_name" {
  type = string
}

variable "visibility_timeout_seconds" {
  type = number
}

variable "message_retention_seconds" {
  type = number
}

variable "receive_wait_time_seconds" {
  type = number
}

variable "max_receive_count" {
  type = number
}

variable "common_tags" {
  type = map(string)
}
