variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.30"
}

variable "node_instance_type" {
  description = "EKS node instance type"
  type        = string
  default     = "t3.micro"
}

variable "node_desired_size" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 3
}

variable "node_min_size" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_instance_count" {
  description = "Number of RDS instances"
  type        = number
  default     = 2
}

variable "postgres_version" {
  description = "PostgreSQL version"
  type        = string
  default     = "15.18"
}

variable "db_username" {
  description = "RDS master username"
  type        = string
  default     = "airflow"
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default = {
    Project   = "airflow-enterprise"
    ManagedBy = "Terraform"
    CreatedBy = "Platform-Engineering"
  }
}
