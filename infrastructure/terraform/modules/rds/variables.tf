variable "project_name" {}
variable "environment" {}
variable "subnet_ids" {
  type = list(string)
}
variable "security_group_id" {
  type = string
}
variable "db_instance_class" {
  default = "db.t3.micro"
}
variable "db_username" {
  default = "postgres"
}
variable "db_password" {
  sensitive = true
}
