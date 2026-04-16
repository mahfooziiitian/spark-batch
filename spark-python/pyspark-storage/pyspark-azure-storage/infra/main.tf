terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

provider "azuread" {}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    Project     = "pyspark-storage"
    Environment = "dev"
  }
}

# ---------------------------------------------------------------------------
# Storage Account (ADLS Gen2)
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "this" {
  name                     = replace(var.project_name, "-", "")
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  tags = {
    Project     = "pyspark-storage"
    Environment = "dev"
  }
}

# ---------------------------------------------------------------------------
# Blob Container
# ---------------------------------------------------------------------------
resource "azurerm_storage_container" "this" {
  name                 = "spark-data"
  storage_account_id   = azurerm_storage_account.this.id
}

# ---------------------------------------------------------------------------
# Azure AD Application & Service Principal (OAuth access)
# ---------------------------------------------------------------------------
data "azuread_client_config" "current" {}

resource "azuread_application" "this" {
  display_name = "${var.project_name}-sp"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "this" {
  client_id = azuread_application.this.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal_password" "this" {
  service_principal_id = azuread_service_principal.this.id
}

# ---------------------------------------------------------------------------
# Role Assignment – Storage Blob Data Contributor
# ---------------------------------------------------------------------------
resource "azurerm_role_assignment" "blob_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.this.object_id
}
