terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.93.0"
    }
  }

  required_version = ">= 1.1.0"
}

provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "ads" {
  name     = "ads-databricks"
  location = "Germany West Central"
}

resource "azurerm_databricks_workspace" "ads" {
  name                        = "ads-DBW"
  resource_group_name         = azurerm_resource_group.ads.name
  location                    = azurerm_resource_group.ads.location
  sku                         = "premium"
  managed_resource_group_name = "ads-DBW-managed"

  customer_managed_key_enabled = true

  tags = {
    Environment = "Production"
  }
}

resource "azurerm_databricks_workspace_root_dbfs_customer_managed_key" "ads" {
  depends_on = [azurerm_key_vault_access_policy.databricks]

  workspace_id     = azurerm_databricks_workspace.ads.id
  key_vault_key_id = azurerm_key_vault_key.ads.id
}

resource "azurerm_key_vault" "ads" {
  name                = "ads-databricks-keyvault"
  location            = azurerm_resource_group.ads.location
  resource_group_name = azurerm_resource_group.ads.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "premium"

  purge_protection_enabled   = true
  soft_delete_retention_days = 7
}

resource "azurerm_key_vault_key" "ads" {
  depends_on = [azurerm_key_vault_access_policy.terraform]

  name         = "databricks-certificate"
  key_vault_id = azurerm_key_vault.ads.id
  key_type     = "RSA"
  key_size     = 2048

  key_opts = [
    "decrypt",
    "encrypt",
    "sign",
    "unwrapKey",
    "verify",
    "wrapKey",
  ]
}

resource "azurerm_key_vault_access_policy" "terraform" {
  key_vault_id = azurerm_key_vault.ads.id
  tenant_id    = azurerm_key_vault.ads.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  key_permissions = [
    "Get",
    "List",
    "Create",
    "Decrypt",
    "Encrypt",
    "Sign",
    "UnwrapKey",
    "Verify",
    "WrapKey",
    "Delete",
    "Restore",
    "Recover",
    "Update",
    "Purge",
	"GetRotationPolicy",
    "SetRotationPolicy",
  ]
}

resource "azurerm_key_vault_access_policy" "databricks" {
  depends_on = [azurerm_databricks_workspace.ads]

  key_vault_id = azurerm_key_vault.ads.id
  tenant_id    = azurerm_databricks_workspace.ads.storage_account_identity.0.tenant_id
  object_id    = azurerm_databricks_workspace.ads.storage_account_identity.0.principal_id

  key_permissions = [
    "Get",
    "UnwrapKey",
    "WrapKey",
  ]
}