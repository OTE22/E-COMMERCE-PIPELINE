resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-${var.environment}-db-subnet-group"
  subnet_ids = var.subnet_ids

  tags = {
    Name = "${var.project_name}-${var.environment}-db-subnet-group"
  }
}

resource "aws_db_instance" "main" {
  identifier        = "${var.project_name}-${var.environment}-db"
  allocated_storage = 20
  storage_type      = "gp3"
  engine            = "postgres"
  engine_version    = "15.4"
  instance_class    = var.db_instance_class
  db_name           = replace(var.project_name, "-", "_")
  username          = var.db_username
  password          = var.db_password
  
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [var.security_group_id]
  
  skip_final_snapshot    = true # For dev only
  publicly_accessible    = false
  
  tags = {
    Name = "${var.project_name}-${var.environment}-db"
  }
}
