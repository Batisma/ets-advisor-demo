// =============================================================================
// ETS Impact Advisor - Microsoft Fabric Infrastructure
// =============================================================================
// Bicep template for deploying Fabric workspace and supporting Azure resources
// 
// Deploy with:
//   az deployment group create --resource-group your-rg --template-file fabric_workspace.bicep
// =============================================================================

@description('Name of the Fabric workspace')
param workspaceName string = 'ETS-Advisor-Demo'

@description('Location for all resources')
param location string = resourceGroup().location

@description('Environment (dev, staging, prod)')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('Fabric capacity SKU')
@allowed(['F2', 'F4', 'F8', 'F16', 'F32', 'F64', 'F128', 'F256', 'F512'])
param fabricCapacitySku string = 'F64'

@description('Event Hub namespace SKU')
@allowed(['Basic', 'Standard', 'Premium'])
param eventHubSku string = 'Standard'

@description('Storage account SKU')
@allowed(['Standard_LRS', 'Standard_GRS', 'Standard_RAGRS', 'Premium_LRS'])
param storageAccountSku string = 'Standard_LRS'

@description('Tags to apply to all resources')
param tags object = {
  Project: 'ETS-Impact-Advisor'
  Environment: environment
  ManagedBy: 'Bicep'
  CreatedDate: utcNow('yyyy-MM-dd')
}

// =============================================================================
// Variables
// =============================================================================

var resourcePrefix = 'ets-${environment}'
var lakehouseName = '${workspaceName}_Lakehouse'
var eventHubNamespaceName = '${resourcePrefix}-eventhub-${uniqueString(resourceGroup().id)}'
var storageAccountName = '${resourcePrefix}storage${uniqueString(resourceGroup().id)}'
var keyVaultName = '${resourcePrefix}-kv-${uniqueString(resourceGroup().id)}'
var applicationInsightsName = '${resourcePrefix}-ai-${uniqueString(resourceGroup().id)}'

// =============================================================================
// 1. Microsoft Fabric Capacity
// =============================================================================

resource fabricCapacity 'Microsoft.Fabric/capacities@2023-11-01' = {
  name: '${resourcePrefix}-fabric-capacity'
  location: location
  tags: tags
  sku: {
    name: fabricCapacitySku
    tier: 'Fabric'
  }
  properties: {
    administration: {
      members: [
        // Add your Azure AD user/group object IDs here
        // '12345678-1234-1234-1234-123456789012'
      ]
    }
  }
}

// =============================================================================
// 2. Storage Account for Data Lake
// =============================================================================

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: storageAccountSku
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true  // Enable hierarchical namespace for Data Lake
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
    encryption: {
      services: {
        blob: {
          enabled: true
          keyType: 'Account'
        }
        file: {
          enabled: true
          keyType: 'Account'
        }
      }
      keySource: 'Microsoft.Storage'
    }
  }
}

// Blob services for storage account
resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 30
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

// Container for sample data
resource sampleDataContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobServices
  name: 'sample-data'
  properties: {
    publicAccess: 'None'
  }
}

// Container for processed data
resource processedDataContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobServices
  name: 'processed-data'
  properties: {
    publicAccess: 'None'
  }
}

// =============================================================================
// 3. Event Hub Namespace for Real-time Data
// =============================================================================

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2023-01-01-preview' = {
  name: eventHubNamespaceName
  location: location
  tags: tags
  sku: {
    name: eventHubSku
    tier: eventHubSku
    capacity: eventHubSku == 'Premium' ? 1 : 1
  }
  properties: {
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
    disableLocalAuth: false
    zoneRedundant: false
    isAutoInflateEnabled: eventHubSku == 'Standard'
    maximumThroughputUnits: eventHubSku == 'Standard' ? 20 : 0
  }
}

// Event Hub for telematics data
resource telematicsEventHub 'Microsoft.EventHub/namespaces/eventhubs@2023-01-01-preview' = {
  parent: eventHubNamespace
  name: 'telematics-trip-end'
  properties: {
    messageRetentionInDays: 7
    partitionCount: 4
    status: 'Active'
  }
}

// Consumer group for ETS Advisor application
resource consumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2023-01-01-preview' = {
  parent: telematicsEventHub
  name: 'ets-advisor-consumer'
  properties: {
    userMetadata: 'Consumer group for ETS Impact Advisor application'
  }
}

// Authorization rule for sending data
resource eventHubSendRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2023-01-01-preview' = {
  parent: telematicsEventHub
  name: 'SendRule'
  properties: {
    rights: ['Send']
  }
}

// Authorization rule for listening to data
resource eventHubListenRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2023-01-01-preview' = {
  parent: telematicsEventHub
  name: 'ListenRule'
  properties: {
    rights: ['Listen']
  }
}

// =============================================================================
// 4. Key Vault for Secrets Management
// =============================================================================

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: tenant().tenantId
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: true
    enableRbacAuthorization: true
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// Store Event Hub connection strings in Key Vault
resource eventHubSendConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'EventHub-Send-ConnectionString'
  properties: {
    value: eventHubSendRule.listKeys().primaryConnectionString
    contentType: 'connection-string'
  }
}

resource eventHubListenConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'EventHub-Listen-ConnectionString'
  properties: {
    value: eventHubListenRule.listKeys().primaryConnectionString
    contentType: 'connection-string'
  }
}

// Store Storage Account connection string
resource storageConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'Storage-ConnectionString'
  properties: {
    value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${storageAccount.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}'
    contentType: 'connection-string'
  }
}

// =============================================================================
// 5. Application Insights for Monitoring
// =============================================================================

resource applicationInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: applicationInsightsName
  location: location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalyticsWorkspace.id
    IngestionMode: 'LogAnalytics'
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

// Log Analytics Workspace for Application Insights
resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${resourcePrefix}-logs'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
    features: {
      searchVersion: 1
      legacy: 0
      enableLogAccessUsingOnlyResourcePermissions: true
    }
  }
}

// =============================================================================
// 6. Service Principal for Fabric Access (Optional)
// =============================================================================

// Note: Service Principal creation requires additional permissions
// This would typically be done separately or via Azure CLI/PowerShell

// =============================================================================
// 7. Role Assignments
// =============================================================================

// Storage Blob Data Contributor role for Fabric workspace
resource storageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, 'StorageBlobDataContributor', workspaceName)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Storage Blob Data Contributor
    principalId: fabricCapacity.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Event Hub Data Sender role
resource eventHubRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventHubNamespace.id, 'EventHubDataSender', workspaceName)
  scope: eventHubNamespace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2b629674-e913-4c01-ae53-ef4638d8f975') // Azure Event Hubs Data Sender
    principalId: fabricCapacity.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// =============================================================================
// 8. Outputs
// =============================================================================

@description('Name of the created Fabric capacity')
output fabricCapacityName string = fabricCapacity.name

@description('ID of the created Fabric capacity')
output fabricCapacityId string = fabricCapacity.id

@description('Name of the storage account')
output storageAccountName string = storageAccount.name

@description('Primary endpoint of the storage account')
output storageAccountPrimaryEndpoint string = storageAccount.properties.primaryEndpoints.blob

@description('Name of the Event Hub namespace')
output eventHubNamespaceName string = eventHubNamespace.name

@description('Name of the telematics Event Hub')
output telematicsEventHubName string = telematicsEventHub.name

@description('Name of the Key Vault')
output keyVaultName string = keyVault.name

@description('URI of the Key Vault')
output keyVaultUri string = keyVault.properties.vaultUri

@description('Name of the Application Insights component')
output applicationInsightsName string = applicationInsights.name

@description('Instrumentation key for Application Insights')
output applicationInsightsInstrumentationKey string = applicationInsights.properties.InstrumentationKey

@description('Connection string for Application Insights')
output applicationInsightsConnectionString string = applicationInsights.properties.ConnectionString

@description('Lakehouse connection details')
output lakehouseDetails object = {
  workspaceName: workspaceName
  lakehouseName: lakehouseName
  storageAccountName: storageAccount.name
  containerNames: {
    sampleData: sampleDataContainer.name
    processedData: processedDataContainer.name
  }
}

@description('Event Hub connection details')
output eventHubDetails object = {
  namespaceName: eventHubNamespace.name
  eventHubName: telematicsEventHub.name
  consumerGroupName: consumerGroup.name
}

@description('Deployment summary')
output deploymentSummary object = {
  resourceGroup: resourceGroup().name
  location: location
  environment: environment
  fabricCapacitySku: fabricCapacitySku
  deploymentTimestamp: utcNow()
  resourcesCreated: {
    fabricCapacity: fabricCapacity.name
    storageAccount: storageAccount.name
    eventHubNamespace: eventHubNamespace.name
    keyVault: keyVault.name
    applicationInsights: applicationInsights.name
    logAnalyticsWorkspace: logAnalyticsWorkspace.name
  }
}

// =============================================================================
// Deployment Notes:
// =============================================================================
/*
Post-deployment steps:

1. Create Fabric Workspace:
   - Navigate to Fabric portal (app.fabric.microsoft.com)
   - Create new workspace: "ETS-Advisor-Demo"
   - Assign to the deployed Fabric capacity

2. Create Lakehouse:
   - In the Fabric workspace, create a new Lakehouse
   - Name: "ets_advisor_lakehouse"
   - This will automatically create OneLake storage

3. Configure Data Ingestion:
   - Use the Event Hub connection strings from Key Vault
   - Configure Kafka connectors or Spark streaming jobs
   - Point to the created Event Hub: "telematics-trip-end"

4. Set up Power BI:
   - Connect Power BI Desktop to the Fabric workspace
   - Use Direct Lake mode for the lakehouse tables
   - Import the PBIP project files

5. Configure Monitoring:
   - Application Insights is configured for telemetry
   - Set up alerts and dashboards as needed

6. Security Configuration:
   - Review and configure access policies
   - Set up Azure AD authentication
   - Configure network security if needed

Resources created:
- Microsoft Fabric Capacity (F64 SKU)
- Storage Account with Data Lake Gen2
- Event Hub Namespace with telematics hub
- Key Vault for secrets management
- Application Insights for monitoring
- Log Analytics Workspace
- Required role assignments

Total estimated cost: ~€2,000-3,000/month for F64 capacity
*/ 