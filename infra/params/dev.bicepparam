using '../main.bicep'

env = 'dev'
suffix = 'ord01'
location = 'centralindia'
tags = {
  project: 'serverless-order-pipeline'
  owner: 'platform-team'
  environment: 'dev'
  costCentre: 'platform-001'
  dataClassification: 'internal'
}
