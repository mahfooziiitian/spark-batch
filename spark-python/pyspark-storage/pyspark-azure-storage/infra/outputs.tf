output "storage_account_name" {
  description = "Name of the Azure Storage Account"
  value       = azurerm_storage_account.this.name
}

output "storage_account_key" {
  description = "Primary access key for the Storage Account"
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}

output "container_name" {
  description = "Name of the blob container"
  value       = azurerm_storage_container.this.name
}

output "client_id" {
  description = "Application (client) ID of the service principal"
  value       = azuread_application.this.client_id
  sensitive   = true
}

output "client_secret" {
  description = "Password / secret for the service principal"
  value       = azuread_service_principal_password.this.value
  sensitive   = true
}

output "tenant_id" {
  description = "Azure AD tenant ID"
  value       = data.azuread_client_config.current.tenant_id
}
