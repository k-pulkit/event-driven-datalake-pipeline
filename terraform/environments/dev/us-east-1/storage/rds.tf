# or ABOUTME: Terraform configuration for the RDS PostgreSQL database in the Default VPC with KMS encryption and cost-optimized settings

# ==========================================
# 1. DATABASE PASSWORD GENERATION
# ==========================================
resource "random_password" "db_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# ==========================================
# 2. DB SUBNET GROUP (Using Default Subnets)
# ==========================================
resource "aws_db_subnet_group" "default" {
  name        = "${var.namespace}-analytics-${var.environment}-rds-subnet-group"
  description = "Subnet group for analytics shared RDS PostgreSQL"
  subnet_ids  = data.aws_subnets.default.ids
  tags        = local.tags
}

# ==========================================
# 3. SECURITY GROUP (Default VPC)
# ==========================================
resource "aws_security_group" "rds_sg" {
  name        = "${var.namespace}-analytics-${var.environment}-rds-sg"
  description = "Security group for analytics shared RDS PostgreSQL database"
  vpc_id      = data.aws_vpc.default.id

  # Ingress: Allow PostgreSQL traffic from anywhere for temporary dev accessibility
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Egress: Allow all outbound
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

# ==========================================
# 4. RDS DB INSTANCE (Cost Optimized & Encrypted)
# ==========================================
resource "aws_db_instance" "postgres" {
  identifier             = "${var.namespace}-analytics-${var.environment}-rds"
  engine                 = "postgres"
  engine_version         = "15"
  instance_class         = "db.t4g.micro"
  allocated_storage      = 20
  max_allocated_storage  = 100
  storage_type           = "gp3"
  db_name                = "gold_db"
  username               = "dbadmin"
  password               = random_password.db_password.result
  db_subnet_group_name   = aws_db_subnet_group.default.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]

  # Enforce KMS Storage Encryption
  storage_encrypted = true
  kms_key_id        = local.kms_master_key_arn

  # Cost Efficiency, Dev Testing & Quick Cleanup settings
  publicly_accessible = true
  skip_final_snapshot = true

  tags = local.tags
}
