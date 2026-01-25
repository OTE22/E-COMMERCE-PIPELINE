variable "project_name" {}
variable "environment" {}
variable "vpc_id" {
  type = string
}
variable "public_subnet_ids" {
  type = list(string)
}
variable "security_group_id" {
  type = string
}
