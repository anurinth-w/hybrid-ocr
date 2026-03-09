terraform {
  backend "s3" {
    bucket         = "hybrid-ocr-terraform-state-anurinth"
    key            = "hybrid-ocr/dev/terraform.tfstate"
    region         = "ap-southeast-1"
    dynamodb_table = "hybrid-ocr-terraform-lock"
    encrypt        = true
  }
}
