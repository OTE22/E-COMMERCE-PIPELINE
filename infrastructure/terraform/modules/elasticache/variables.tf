variable "project_name" {}
variable "environment" {}
variable "subnet_ids" {
  type = list(string)
}
variable "security_group_id" {
  type = string
}
variable "redis_node_type" {
  default = "cache.t4g.micro"
}
