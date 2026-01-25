variable "project_name" {}
variable "environment" {}
variable "aws_region" {}
variable "vpc_id" {}
variable "private_subnet_ids" {
  type = list(string)
}
variable "security_group_id" {
  type = string
}
variable "target_group_arn" {
  type = string
}
variable "db_endpoint" {}
variable "db_username" {
  default = "postgres"
}
variable "db_password" {
  sensitive = true
}
variable "redis_endpoint" {}
variable "redis_port" {}
variable "container_image" {
  description = "Docker image to deploy"
  default     = "nginx:latest" # Placeholder
}
variable "task_cpu" {
  default = "256"
}
variable "task_memory" {
  default = "512"
}
variable "service_desired_count" {
  default = 1
}
