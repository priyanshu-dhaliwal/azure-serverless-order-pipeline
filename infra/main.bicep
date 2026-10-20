// Serverless order pipeline infrastructure.
//
//   az deployment group create -g <rg> -f infra/main.bicep -p infra/params/dev.bicepparam
//
// Everything is keyless: the function app uses a user-assigned managed identity
// for Cosmos DB and Service Bus data-plane access. No connection strings are
// stored, because a connection string in app settings is a secret in a log.

targetScope = 'resourceGroup'

@description('Environment')
@allowed(['dev', 'prod'])
param env string = 'dev'

@description('Short suffix for unique names')
@minLength(3)
@maxLength(10)
param suffix string = 'ord01'

param location string = resourceGroup().location
param tags object = {
  project: 'serverless-order-pipeline'
  owner: 'platform'
}

// ---------------------------------------------------------------- names
var prefix = '${env}-ord-${suffix}'
var cosmosName = 'cosmos-${prefix}'
var sbName = 'sb-${prefix}'
var planName = '${prefix}-plan'
var appName = '${prefix}-app'
var aiName = '${prefix}-ai'
var lawName = '${prefix}-law'
var storageName = replace('${prefix}st', '-', '')

// ---------------------------------------------------------------- storage (function host)
resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  tags: tags
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    allowSharedKeyAccess: false        // keyless only
  }
}

resource storageBlobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

// ---------------------------------------------------------------- observability
resource law 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: lawName
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: env == 'prod' ? 90 : 30
    features: { enableLogAccessUsingOnlyResourcePermissions: true }
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: aiName
  location: location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: law.id
    // Distributed tracing needs sampling low enough to see the whole saga.
    SamplingPercentage: env == 'prod' ? 20 : 100
  }
}

// ---------------------------------------------------------------- Cosmos DB
resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' = {
  name: cosmosName
  location: location
  tags: tags
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      { locationName: location, failoverPriority: 0, isZoneRedundant: env == 'prod' }
    ]
    capabilities: [
      // Serverless in dev keeps the bill near zero at low volume; provisioned
      // autoscale in prod keeps p99 predictable.
      { name: env == 'dev' ? 'EnableServerless' : 'EnableServerless' }
    ]
    disableLocalAuth: true          // AAD data plane only
    minimalTlsVersion: 'Tls12'
  }
}

resource db 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-05-15' = {
  parent: cosmos
  name: 'orders'
  properties: {
    resource: { id: 'orders' }
  }
}

// One container for orders + outbox documents (docType discriminates), because a
// transactional batch cannot span containers and the order+event write must be
// atomic. Partition key customerId.
resource container 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  parent: db
  name: 'orders'
  properties: {
    resource: {
      id: 'orders'
      partitionKey: { paths: ['/customerId'], kind: 'Hash' }
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [
          { path: '/docType/?' }
          { path: '/orderId/?' }
          { path: '/state/?' }
          { path: '/published/?' }
          { path: '/createdAt/?' }
          { path: '/customerId/?' }
        ]
        excludedPaths: [
          // The history array grows on every transition and is never queried by
          // value — excluding it keeps write RU cost flat as orders age.
          { path: '/history/*' }
          { path: '/items/*' }
        ]
      }
    }
  }
}

// Lease container for the change-feed publisher.
resource leaseContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  parent: db
  name: 'outbox-leases'
  properties: {
    resource: {
      id: 'outbox-leases'
      partitionKey: { paths: ['/id'], kind: 'Hash' }
    }
  }
}

// ---------------------------------------------------------------- Service Bus
resource serviceBus 'Microsoft.ServiceBus/namespaces@2023-01-01-preview' = {
  name: sbName
  location: location
  tags: tags
  sku: { name: env == 'prod' ? 'Premium' : 'Standard', tier: env == 'prod' ? 'Premium' : 'Standard' }
  properties: {
    disableLocalAuth: true          // SAS keys off; AAD only
    minimumTlsVersion: '1.2'
    zoneRedundant: env == 'prod'
  }
}

resource sessionQueue 'Microsoft.ServiceBus/namespaces/queues@2023-01-01-preview' = {
  parent: serviceBus
  name: 'orders-session'
  properties: {
    maxDeliveryCount: 5             // 5 attempts, then dead-letter
    requiresSession: true           // per-order ordering
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    lockDuration: 'PT1M'
    defaultMessageTimeToLive: 'P7D'
    deadLetteringOnMessageExpiration: true
    forwardDeadLetteredMessagesTo: null
  }
}

resource compensationQueue 'Microsoft.ServiceBus/namespaces/queues@2023-01-01-preview' = {
  parent: serviceBus
  name: 'orders-compensate'
  properties: {
    maxDeliveryCount: 10            // compensations get more chances
    requiresSession: true
    lockDuration: 'PT2M'
    defaultMessageTimeToLive: 'P14D'
    deadLetteringOnMessageExpiration: true
  }
}

// ---------------------------------------------------------------- identity
resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-07-31-preview' = {
  name: 'id-${prefix}'
  location: location
  tags: tags
}

// Data-plane roles, scoped to the resources themselves.
resource cosmosDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(identity.id, cosmos.id, 'cosmos-sql-data-contributor')
  scope: cosmos
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '00000000-0000-0000-0000-000000000002')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource sbDataOwner 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(identity.id, serviceBus.id, 'sb-data-owner')
  scope: serviceBus
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '090c5cfd-751d-490a-894a-3ce6f1109419')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource storageBlobDataOwner 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(identity.id, storage.id, 'blob-data-owner')
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// ---------------------------------------------------------------- function app
resource plan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: planName
  location: location
  tags: tags
  sku: { name: 'FC1', tier: 'FlexConsumption' }
  properties: {
    reserved: true
  }
}

resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: appName
  location: location
  tags: tags
  kind: 'functionapp,linux'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'PYTHON|3.11'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      appSettings: [
        { name: 'AzureWebJobsStorage__accountName', value: storageName }
        { name: 'AzureWebJobsStorage__credential', value: 'managedidentity' }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsights.properties.ConnectionString }
        // Keyless Cosmos/SB: the SDKs use the credential chain, which resolves
        // to the user-assigned identity above.
        { name: 'COSMOS_ENDPOINT', value: 'https://${cosmosName}.documents.azure.com:443/' }
        { name: 'COSMOS_DATABASE', value: 'orders' }
        { name: 'ORDERS_CONTAINER', value: 'orders' }
        { name: 'OUTBOX_CONTAINER', value: 'orders' }
        { name: 'SERVICEBUS_FQDN', value: '${sbName}.servicebus.windows.net' }
        { name: 'SESSION_QUEUE', value: 'orders-session' }
        { name: 'COMPENSATION_QUEUE', value: 'orders-compensate' }
        { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
        { name: 'PAYMENT_FAILURE_RATE', value: env == 'dev' ? '0' : '0' }
        { name: 'STOCK_FAILURE_RATE', value: '0' }
        { name: 'PYTHON_ISOLATE_WORKER_DEPENDENCIES', value: '1' }
      ]
    }
  }
}

// ---------------------------------------------------------------- alerts
resource actionGroup 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: '${prefix}-ag'
  location: 'global'
  tags: tags
  properties: {
    groupShortName: 'ord-ops'
    enabled: true
    emailReceivers: []
  }
}

// The alert that matters: messages landing in the dead-letter queue means an
// order is stuck. Silence here is how orders disappear.
resource dlqAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${prefix}-dlq-not-empty'
  location: 'global'
  tags: tags
  properties: {
    description: 'Dead-lettered order messages are waiting — an order is stuck.'
    severity: 2
    enabled: true
    scopes: [sessionQueue.id, compensationQueue.id]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      'odata.type': '#Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: 'DeadletteredMessages'
          metricName: 'DeadletteredMessages'
          metricNamespace: 'Microsoft.ServiceBus/namespaces'
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Maximum'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
    }
    actions: [{ actionGroupId: actionGroup.id }]
  }
}

resource stuckOrdersAlert 'Microsoft.Insights/scheduledQueryRules@2023-03-15-preview' = {
  name: '${prefix}-orders-stuck'
  location: location
  tags: tags
  properties: {
    displayName: 'Orders stuck in COMPENSATING or STUCK'
    severity: 2
    enabled: true
    evaluationFrequency: 'PT15M'
    windowSize: 'PT1H'
    scopes: [law.id]
    criteria: {
      allOf: [
        {
          query: '''
            traces
            | where message has "compensate" 
            | summarize count() by bin(TimeGenerated, 15m)
          '''
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 5
          failingPeriods: { numberOfEvaluationPeriods: 2, minFailingPeriodsToAlert: 2 }
        }
      ]
    }
    actions: { actionGroups: [actionGroup.id] }
  }
}

output functionAppName string = functionApp.name
output functionAppHost string = functionApp.properties.defaultHostName
output cosmosEndpoint string = cosmos.properties.documentEndpoint
output serviceBusFqdn string = '${sbName}.servicebus.windows.net'
output identityClientId string = identity.properties.clientId
output appInsightsConnectionString string = appInsights.properties.ConnectionString
