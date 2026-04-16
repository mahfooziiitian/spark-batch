variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus"
}

variable "project_name" {
  description = "Project name used to derive resource names"
  type        = string
  default     = "pyspark-azure"
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "rg-pyspark-storage"
}
