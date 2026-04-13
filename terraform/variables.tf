variable "environment" {
  description = "The environment deploy tier (e.g., dev, prod, local)"
  type        = string
  default     = "local"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}
