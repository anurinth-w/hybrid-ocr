output "api_policy_arn" {
  value = aws_iam_policy.api_policy.arn
}

output "worker_policy_arn" {
  value = aws_iam_policy.worker_policy.arn
}
