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

resource "azurerm_resource_group" "ads-rg" {
  name     = "ads-databricks"
  location = "Germany West Central"
}

resource "azurerm_databricks_workspace" "ads-databricks" {
  name                        = "ads-workspace"
  resource_group_name         = azurerm_resource_group.ads-rg.name
  location                    = azurerm_resource_group.ads-rg.location
  sku                         = "trial"
  public_network_access_enabled = true
}

output "databricks_host" {
  value = "https://${azurerm_databricks_workspace.ads-databricks.workspace_url}/"
}