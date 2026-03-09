variable "table_name" {
  type = string
}

variable "hash_key" {
  type    = string
  default = "job_id"
}

variable "billing_mode" {
  type    = string
  default = "PAY_PER_REQUEST"
}

variable "enable_pitr" {
  type    = bool
  default = true
}

variable "common_tags" {
  type = map(string)
}
